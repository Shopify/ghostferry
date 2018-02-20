package ghostferry

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type ReverifyEntry struct {
	Pk    uint64
	Table *schema.Table
}

type reverifyEntryBatch struct {
	Pks   []uint64
	Table *schema.Table
}

type ReverifyStore map[string]map[ReverifyEntry]struct{}

func (r ReverifyStore) Add(entry ReverifyEntry) {
	table := entry.Table.String()

	if _, exists := r[table]; !exists {
		r[table] = make(map[ReverifyEntry]struct{})
	}

	r[table][entry] = struct{}{}
}

func (r ReverifyStore) Pks() map[*schema.Table][]uint64 {
	res := make(map[*schema.Table][]uint64)

	for _, entriesMap := range r {
		entries := make([]ReverifyEntry, 0, len(entriesMap))
		for entry, _ := range entriesMap {
			entries = append(entries, entry)
		}

		table := entries[0].Table
		pks := make([]uint64, len(entries))
		for idx, entry := range entries {
			pks[idx] = entry.Pk
		}

		res[table] = pks
	}

	return res
}

type verificationResultAndError struct {
	Result *VerificationResult
	Error  error
}

func (r verificationResultAndError) ErroredOrFailed() bool {
	return r.Result == nil || !r.Result.DataCorrect
}

type IterativeVerifier struct {
	CursorConfig   *CursorConfig
	BinlogStreamer *BinlogStreamer
	SourceDB       *sql.DB
	TargetDB       *sql.DB

	Tables           []*schema.Table
	IgnoredTables    []string
	DatabaseRewrites map[string]string
	TableRewrites    map[string]string
	Concurrency      int

	reverifyStore ReverifyStore
	reverifyChan  chan ReverifyEntry
	logger        *logrus.Entry

	beforeCutoverVerifyDone    bool
	verifyDuringCutoverStarted AtomicBoolean
	wg                         *sync.WaitGroup

	// Variables for verification in the background
	verificationResult       *VerificationResult
	verificationErr          error
	backgroundVerificationWg *sync.WaitGroup
	backgroundStartTime      time.Time
	backgroundDoneTime       time.Time
}

func (v *IterativeVerifier) SanityCheckParameters() error {
	if v.CursorConfig == nil {
		return errors.New("CursorConfig must not be nil")
	}

	if v.BinlogStreamer == nil {
		return errors.New("BinlogStreamer must not be nil")
	}

	if v.SourceDB == nil {
		return errors.New("SourceDB must not be nil")
	}

	if v.TargetDB == nil {
		return errors.New("TargetDB must not be nil")
	}

	if v.Concurrency <= 0 {
		return fmt.Errorf("iterative verifier concurrency must be greater than 0, not %d", v.Concurrency)
	}

	return nil
}

func (v *IterativeVerifier) Initialize() error {
	v.logger = logrus.WithField("tag", "iterative_verifier")

	if err := v.SanityCheckParameters(); err != nil {
		v.logger.WithError(err).Error("iterative verifier parameter sanity check failed")
		return err
	}

	v.reverifyStore = make(ReverifyStore)
	v.reverifyChan = make(chan ReverifyEntry)
	return nil
}

func (v *IterativeVerifier) VerifyBeforeCutover() error {
	v.logger.Info("starting pre-cutover verification")

	v.wg = &sync.WaitGroup{}
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		v.consumeReverifyChan()
	}()
	v.BinlogStreamer.AddEventListener(v.binlogEventListener)

	pool := &WorkerPool{
		Concurrency: v.Concurrency,
		LogTag:      "iterative_verifier",
		Process: func(tableInterface interface{}) (interface{}, error) {
			table := tableInterface.(*schema.Table)

			err := v.verifyTableBeforeCutover(table)
			if err != nil {
				v.logger.WithError(err).Error("error occured during verify table before cutover")
			}
			return nil, err
		},
	}

	tables := make([]interface{}, len(v.Tables))
	for i, table := range v.Tables {
		tables[i] = table
	}

	_, errs := pool.Run(tables)

	// Get the first err and return that if not nil
	var err error
	for _, err = range errs {
		if err != nil {
			break
		}
	}

	// TODO: we can reduce the cutover phase (downtime) drastically by eagerly
	// running re-verification on the ReverifyStore a few times at this point

	v.beforeCutoverVerifyDone = true
	v.logger.Info("pre-cutover verification complete")

	return err
}

func (v *IterativeVerifier) VerifyDuringCutover() (*VerificationResult, error) {
	// Since no more reverify batch can be sent at this point,
	// we should ensure nothing can be actually added to the reverifyStore
	// by spinning down the consumeReverifyChan go routine.
	v.verifyDuringCutoverStarted.Set(true)
	close(v.reverifyChan)
	v.wg.Wait()

	erroredOrFailed := errors.New("reverify errored or failed")

	v.logger.Info("starting verification during cutover")
	pool := &WorkerPool{
		Concurrency: v.Concurrency,
		LogTag:      "iterative_verifier",
		Process: func(reverifyBatchInterface interface{}) (interface{}, error) {
			reverifyBatch := reverifyBatchInterface.(reverifyEntryBatch)
			v.logger.WithFields(logrus.Fields{
				"table":    reverifyBatch.Table.String(),
				"len(pks)": len(reverifyBatch.Pks),
			}).Debug("received pk batch to reverify")

			verificationResult, err := v.verifyPksDuringCutover(reverifyBatch.Table, reverifyBatch.Pks)
			resultAndErr := verificationResultAndError{verificationResult, err}
			if resultAndErr.ErroredOrFailed() {
				if resultAndErr.Error != nil {
					v.logger.WithError(resultAndErr.Error).Error("error occured in verification during cutover")
				} else {
					v.logger.Errorf("failed verification: %s", resultAndErr.Result.Message)
				}

				return resultAndErr, erroredOrFailed
			}

			return resultAndErr, nil
		},
	}

	allBatches := make([]interface{}, 0)
	for table, pks := range v.reverifyStore.Pks() {
		for i := 0; i < len(pks); i += int(v.CursorConfig.BatchSize) {
			lastIdx := i + int(v.CursorConfig.BatchSize)
			if lastIdx > len(pks) {
				lastIdx = len(pks)
			}

			reverifyBatch := reverifyEntryBatch{Pks: pks[i:lastIdx], Table: table}
			allBatches = append(allBatches, reverifyBatch)
		}
	}

	results, _ := pool.Run(allBatches)

	var result *VerificationResult
	var err error
	for i := 0; i < v.Concurrency; i++ {
		if results[i] == nil {
			// This means the worker pool exitted early and another goroutine
			// must have returned and error.
			continue
		}

		resultAndErr := results[i].(verificationResultAndError)
		result = resultAndErr.Result
		err = resultAndErr.Error

		if resultAndErr.ErroredOrFailed() {
			break
		}
	}

	v.logger.Info("cutover verification complete")

	return result, err
}

func (v *IterativeVerifier) StartInBackground() error {
	if v.logger == nil {
		return errors.New("Initialize() must be called before this")
	}

	if !v.beforeCutoverVerifyDone {
		return errors.New("VerifyBeforeCutover() must be called before this")
	}

	if v.verifyDuringCutoverStarted.Get() {
		return errors.New("verification during cutover has already been started")
	}

	v.verificationResult = nil
	v.verificationErr = nil
	v.backgroundVerificationWg = &sync.WaitGroup{}
	v.backgroundStartTime = time.Now()
	v.backgroundDoneTime = time.Time{}

	v.logger.Info("starting iterative verification in the background")

	v.backgroundVerificationWg.Add(1)
	go func() {
		defer func() {
			v.backgroundDoneTime = time.Now()
			v.backgroundVerificationWg.Done()
		}()

		v.verificationResult, v.verificationErr = v.VerifyDuringCutover()
	}()

	return nil
}

func (v *IterativeVerifier) Wait() {
	v.backgroundVerificationWg.Wait()
}

func (v *IterativeVerifier) IsStarted() bool {
	return v.verifyDuringCutoverStarted.Get()
}

func (v *IterativeVerifier) StartTime() time.Time {
	return v.backgroundStartTime
}

func (v *IterativeVerifier) IsDone() bool {
	return !v.backgroundDoneTime.IsZero()
}

func (v *IterativeVerifier) DoneTime() time.Time {
	return v.backgroundDoneTime
}

func (v *IterativeVerifier) VerificationResult() (*VerificationResult, error) {
	return v.verificationResult, v.verificationErr
}

func (v *IterativeVerifier) verifyTableBeforeCutover(table *schema.Table) error {
	// The cursor will stop iterating when it cannot find anymore rows,
	// so it will not iterate until MaxUint64.
	cursor := v.CursorConfig.NewCursorWithoutRowLock(table, math.MaxUint64)

	// It only needs the PKs, not the entire row.
	cursor.ColumnsToSelect = []string{fmt.Sprintf("`%s`", table.GetPKColumn(0).Name)}
	return cursor.Each(func(batch *RowBatch) error {
		pks := make([]uint64, 0, batch.Size())

		for _, rowData := range batch.Values() {
			pk, err := rowData.GetUint64(batch.PkIndex())
			if err != nil {
				return err
			}

			pks = append(pks, pk)
		}

		mismatchedPks, err := v.compareFingerprints(pks, batch.TableSchema())
		if err != nil {
			v.logger.WithError(err).Errorf("failed to fingerprint table %s", batch.TableSchema().String())
			return err
		}

		if len(mismatchedPks) > 0 {
			v.logger.WithFields(logrus.Fields{
				"table":          batch.TableSchema().String(),
				"mismatched_pks": mismatchedPks,
			}).Info("mismatched rows will be re-verified")

			for _, pk := range mismatchedPks {
				v.reverifyChan <- ReverifyEntry{Pk: pk, Table: batch.TableSchema()}
			}
		}

		return nil
	})
}

func (v *IterativeVerifier) verifyPksDuringCutover(table *schema.Table, pks []uint64) (*VerificationResult, error) {
	mismatchedPks, err := v.compareFingerprints(pks, table)
	if err != nil {
		return nil, err
	}

	if len(mismatchedPks) > 0 {
		pkStrings := make([]string, len(mismatchedPks))
		for idx, pk := range mismatchedPks {
			pkStrings[idx] = strconv.FormatUint(pk, 10)
		}

		return &VerificationResult{
			DataCorrect: false,
			Message:     fmt.Sprintf("verification failed on table: %s for pks: %s", table.String(), strings.Join(pkStrings, ",")),
		}, nil
	}

	return &VerificationResult{true, ""}, nil
}

func (v *IterativeVerifier) consumeReverifyChan() {
	for {
		entry, open := <-v.reverifyChan
		if !open {
			return
		}

		v.reverifyStore.Add(entry)
	}
}

func (v *IterativeVerifier) binlogEventListener(evs []DMLEvent) error {
	if v.verifyDuringCutoverStarted.Get() {
		return fmt.Errorf("cutover has started but received binlog event!")
	}

	for _, ev := range evs {
		if v.tableIsIgnored(ev.TableSchema()) {
			continue
		}

		pk, err := ev.PK()
		if err != nil {
			return err
		}

		v.reverifyChan <- ReverifyEntry{Pk: pk, Table: ev.TableSchema()}
	}

	return nil
}

func (v *IterativeVerifier) tableIsIgnored(table *schema.Table) bool {
	for _, ignored := range v.IgnoredTables {
		if table.Name == ignored {
			return true
		}
	}

	return false
}

func (v *IterativeVerifier) compareFingerprints(pks []uint64, table *schema.Table) ([]uint64, error) {
	targetDb := table.Schema
	if targetDbName, exists := v.DatabaseRewrites[targetDb]; exists {
		targetDb = targetDbName
	}

	targetTable := table.Name
	if targetTableName, exists := v.TableRewrites[targetTable]; exists {
		targetTable = targetTableName
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)

	var sourceHashes map[uint64][]byte
	var sourceErr error
	go func() {
		defer wg.Done()
		sourceErr = WithRetries(5, 0, v.logger, "get fingerprints from source db", func() (err error) {
			sourceHashes, err = v.GetHashes(v.SourceDB, table.Schema, table.Name, table.GetPKColumn(0).Name, table.Columns, pks)
			return
		})
	}()

	var targetHashes map[uint64][]byte
	var targetErr error
	go func() {
		defer wg.Done()
		targetErr = WithRetries(5, 0, v.logger, "get fingerprints from target db", func() (err error) {
			targetHashes, err = v.GetHashes(v.TargetDB, targetDb, targetTable, table.GetPKColumn(0).Name, table.Columns, pks)
			return
		})
	}()

	wg.Wait()
	if sourceErr != nil {
		return nil, sourceErr
	}
	if targetErr != nil {
		return nil, targetErr
	}

	return compareHashes(sourceHashes, targetHashes), nil
}

func compareHashes(source, target map[uint64][]byte) []uint64 {
	mismatchSet := map[uint64]struct{}{}

	for pk, targetHash := range target {
		sourceHash, exists := source[pk]
		if !bytes.Equal(sourceHash, targetHash) || !exists {
			mismatchSet[pk] = struct{}{}
		}
	}

	for pk, sourceHash := range source {
		targetHash, exists := target[pk]
		if !bytes.Equal(sourceHash, targetHash) || !exists {
			mismatchSet[pk] = struct{}{}
		}
	}

	mismatches := make([]uint64, 0, len(mismatchSet))
	for mismatch, _ := range mismatchSet {
		mismatches = append(mismatches, mismatch)
	}
	return mismatches
}

func (v *IterativeVerifier) GetHashes(db *sql.DB, schema, table, pkColumn string, columns []schema.TableColumn, pks []uint64) (map[uint64][]byte, error) {
	sql, args, err := GetMd5HashesSql(schema, table, pkColumn, columns, pks)
	if err != nil {
		return nil, err
	}

	// This query must be a prepared query. If it is not, querying will use
	// MySQL's plain text interface, which will scan all values into []uint8
	// if we give it []interface{}.
	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	resultSet := make(map[uint64][]byte)
	for rows.Next() {
		rowData, err := ScanGenericRow(rows, 2)
		if err != nil {
			return nil, err
		}

		pk, err := rowData.GetUint64(0)
		if err != nil {
			return nil, err
		}

		resultSet[pk] = rowData[1].([]byte)
	}
	return resultSet, nil
}

func GetMd5HashesSql(schema, table, pkColumn string, columns []schema.TableColumn, pks []uint64) (string, []interface{}, error) {
	quotedPK := quoteField(pkColumn)
	return rowMd5Selector(columns, pkColumn).
		From(QuotedTableNameFromString(schema, table)).
		Where(sq.Eq{quotedPK: pks}).
		OrderBy(quotedPK).
		ToSql()
}

func rowMd5Selector(columns []schema.TableColumn, pkColumn string) sq.SelectBuilder {
	quotedPK := quoteField(pkColumn)

	hashStrs := make([]string, len(columns))
	for idx, column := range columns {
		quotedCol := normalizeAndQuoteColumn(column)
		hashStrs[idx] = fmt.Sprintf("MD5(COALESCE(%s, 'NULL'))", quotedCol)
	}

	return sq.Select(fmt.Sprintf(
		"%s, MD5(CONCAT(%s)) AS row_fingerprint",
		quotedPK,
		strings.Join(hashStrs, ","),
	))
}

func normalizeAndQuoteColumn(column schema.TableColumn) (quoted string) {
	quoted = quoteField(column.Name)
	if column.Type == schema.TYPE_FLOAT {
		quoted = fmt.Sprintf("(if (%s = '-0', 0, %s))", quoted, quoted)
	}
	return
}
