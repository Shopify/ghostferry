package ghostferry

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	sq "github.com/Masterminds/squirrel"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type ReverifyBatch struct {
	PaginationKeys []interface{}
	Table          TableIdentifier
}

type ReverifyEntry struct {
	PaginationKey string
	Table         *TableSchema
}

type ReverifyStore struct {
	MapStore           map[TableIdentifier]map[string]struct{}
	mapStoreMutex      *sync.Mutex
	BatchStore         []ReverifyBatch
	RowCount           uint64
	EmitLogPerRowCount uint64
}

func NewReverifyStore() *ReverifyStore {
	r := &ReverifyStore{
		mapStoreMutex:      &sync.Mutex{},
		RowCount:           uint64(0),
		EmitLogPerRowCount: uint64(10000),
	}

	r.flushStore()
	return r
}

func (r *ReverifyStore) Add(entry ReverifyEntry) {
	r.mapStoreMutex.Lock()
	defer r.mapStoreMutex.Unlock()

	paginationKeyStr := entry.PaginationKey
	tableId := NewTableIdentifierFromSchemaTable(entry.Table)
	if _, exists := r.MapStore[tableId]; !exists {
		r.MapStore[tableId] = make(map[string]struct{})
	}

	if _, exists := r.MapStore[tableId][paginationKeyStr]; !exists {
		r.MapStore[tableId][paginationKeyStr] = struct{}{}
		r.RowCount++
		if r.RowCount%r.EmitLogPerRowCount == 0 {
			metrics.Gauge("iterative_verifier_store_rows", float64(r.RowCount), []MetricTag{}, 1.0)
			logrus.WithFields(logrus.Fields{
				"tag":  "reverify_store",
				"rows": r.RowCount,
			}).Debug("added rows will be reverified")
		}
	}
}

func (r *ReverifyStore) FlushAndBatchByTable(batchsize int) []ReverifyBatch {
	r.mapStoreMutex.Lock()
	defer r.mapStoreMutex.Unlock()

	r.BatchStore = make([]ReverifyBatch, 0)
	for tableId, paginationKeySet := range r.MapStore {
		paginationKeyBatch := make([]interface{}, 0, batchsize)
		for paginationKeyStr, _ := range paginationKeySet {
			paginationKeyBatch = append(paginationKeyBatch, paginationKeyStr)
			delete(paginationKeySet, paginationKeyStr)
			if len(paginationKeyBatch) >= batchsize {
				r.BatchStore = append(r.BatchStore, ReverifyBatch{
					PaginationKeys: paginationKeyBatch,
					Table:          tableId,
				})
				paginationKeyBatch = make([]interface{}, 0, batchsize)
			}
		}

		if len(paginationKeyBatch) > 0 {
			r.BatchStore = append(r.BatchStore, ReverifyBatch{
				PaginationKeys: paginationKeyBatch,
				Table:          tableId,
			})
		}

		delete(r.MapStore, tableId)
	}

	r.flushStore()
	return r.BatchStore
}

func (r *ReverifyStore) flushStore() {
	r.MapStore = make(map[TableIdentifier]map[string]struct{})
	r.RowCount = 0
}

type verificationResultAndError struct {
	Result VerificationResult
	Error  error
}

func (r verificationResultAndError) ErroredOrFailed() bool {
	return r.Error != nil || !r.Result.DataCorrect
}

type IterativeVerifier struct {
	CompressionVerifier *CompressionVerifier
	CursorConfig        *CursorConfig
	BinlogStreamer      *BinlogStreamer
	TableSchemaCache    TableSchemaCache
	SourceDB            *sql.DB
	TargetDB            *sql.DB

	Tables              []*TableSchema
	IgnoredTables       []string
	IgnoredColumns      map[string]map[string]struct{}
	DatabaseRewrites    map[string]string
	TableRewrites       map[string]string
	Concurrency         int
	MaxExpectedDowntime time.Duration

	reverifyStore *ReverifyStore
	logger        *logrus.Entry

	beforeCutoverVerifyDone    bool
	verifyDuringCutoverStarted AtomicBoolean

	// Variables for verification in the background
	verificationResultAndStatus VerificationResultAndStatus
	verificationErr             error
	backgroundVerificationWg    *sync.WaitGroup
	backgroundStartTime         time.Time
	backgroundDoneTime          time.Time
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

	v.reverifyStore = NewReverifyStore()
	return nil
}

func (v *IterativeVerifier) VerifyOnce() (VerificationResult, error) {
	v.logger.Info("starting one-off verification of all tables")

	err := v.iterateAllTables(func(paginationKey string, tableSchema *TableSchema) error {
		return VerificationResult{
			DataCorrect:     false,
			Message:         fmt.Sprintf("verification failed on table: %s for paginationKey: %s", tableSchema.String(), paginationKey),
			IncorrectTables: []string{tableSchema.String()},
		}
	})

	v.logger.Info("one-off verification complete")

	switch e := err.(type) {
	case VerificationResult:
		return e, nil
	default:
		return NewCorrectVerificationResult(), e
	}
}

func (v *IterativeVerifier) VerifyBeforeCutover() error {
	if v.TableSchemaCache == nil {
		return fmt.Errorf("iterative verifier must be given the table schema cache before starting verify before cutover")
	}

	v.logger.Info("starting pre-cutover verification")

	v.logger.Debug("attaching binlog event listener")
	v.BinlogStreamer.AddEventListener(v.binlogEventListener)

	v.logger.Debug("verifying all tables")
	err := v.iterateAllTables(func(paginationKey string, tableSchema *TableSchema) error {
		v.reverifyStore.Add(ReverifyEntry{PaginationKey: paginationKey, Table: tableSchema})
		return nil
	})

	if err == nil {
		// This reverification phase is to reduce the size of the set of rows
		// that need to be reverified during cutover. Failures during
		// reverification at this point could have been caused by still
		// ongoing writes and we therefore just re-add those rows to the
		// store rather than failing the move prematurely.
		err = v.reverifyUntilStoreIsSmallEnough(30)
	}

	v.logger.Info("pre-cutover verification complete")
	v.beforeCutoverVerifyDone = true

	return err
}

func (v *IterativeVerifier) VerifyDuringCutover() (VerificationResult, error) {
	v.logger.Info("starting verification during cutover")
	v.verifyDuringCutoverStarted.Set(true)
	result, err := v.verifyStore("iterative_verifier_during_cutover", []MetricTag{})
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

	v.verificationResultAndStatus = VerificationResultAndStatus{
		StartTime: time.Now(),
		DoneTime:  time.Time{},
	}
	v.verificationErr = nil
	v.backgroundVerificationWg = &sync.WaitGroup{}

	v.logger.Info("starting iterative verification in the background")

	v.backgroundVerificationWg.Add(1)
	go func() {
		defer func() {
			v.backgroundDoneTime = time.Now()
			v.backgroundVerificationWg.Done()
		}()

		v.verificationResultAndStatus.VerificationResult, v.verificationErr = v.VerifyDuringCutover()
		v.verificationResultAndStatus.DoneTime = time.Now()
	}()

	return nil
}

func (v *IterativeVerifier) Wait() {
	v.backgroundVerificationWg.Wait()
}

func (v *IterativeVerifier) Message() string {
	return ""
}

func (v *IterativeVerifier) Result() (VerificationResultAndStatus, error) {
	return v.verificationResultAndStatus, v.verificationErr
}

func (v *IterativeVerifier) GetHashes(db *sql.DB, schemaName, tableName, paginationKeyColumn string, columns []schema.TableColumn, paginationKeys []interface{}) (map[string][]byte, error) {
	table := v.TableSchemaCache.Get(schemaName, tableName)
	paginationColumns := table.GetPaginationColumns()

	sql, args, err := GetMd5HashesSql(schemaName, tableName, paginationColumns, columns, paginationKeys)
	if err != nil {
		return nil, err
	}

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

	resultSet := make(map[string][]byte)
	numPaginationCols := len(paginationColumns)

	for rows.Next() {
		// Scan: pagination_col1, pagination_col2, ..., row_fingerprint
		rowData, err := ScanGenericRow(rows, numPaginationCols+1)
		if err != nil {
			return nil, err
		}

		// Build pagination key from columns (works for both single and composite keys)
		keys := make([]PaginationKey, len(paginationColumns))
		for i, paginationColumn := range paginationColumns {
			switch paginationColumn.Type {
			case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
				paginationKeyUint, err := rowData.GetUint64(i)
				if err != nil {
					return nil, err
				}
				keys[i] = NewUint64Key(paginationKeyUint)

			case schema.TYPE_BINARY, schema.TYPE_STRING:
				paginationKeyBytes, ok := rowData[i].([]byte)
				if !ok {
					return nil, fmt.Errorf("expected []byte for binary pagination key, got %T", rowData[i])
				}
				keys[i] = NewBinaryKey(paginationKeyBytes)

			default:
				paginationKeyUint, err := rowData.GetUint64(i)
				if err != nil {
					return nil, err
				}
				keys[i] = NewUint64Key(paginationKeyUint)
			}
		}

		// For single column, use the key directly; for composite, wrap in CompositeKey
		var paginationKeyStr string
		if len(keys) == 1 {
			paginationKeyStr = keys[0].String()
		} else {
			paginationKeyStr = CompositeKey(keys).String()
		}

		resultSet[paginationKeyStr] = rowData[numPaginationCols].([]byte)
	}
	return resultSet, nil
}

func (v *IterativeVerifier) reverifyUntilStoreIsSmallEnough(maxIterations int) error {
	var timeToVerify time.Duration

	for iteration := 0; iteration < maxIterations; iteration++ {
		before := v.reverifyStore.RowCount
		start := time.Now()

		_, err := v.verifyStore("reverification_before_cutover", []MetricTag{{"iteration", strconv.FormatInt(int64(iteration), 10)}})
		if err != nil {
			return err
		}

		after := v.reverifyStore.RowCount
		timeToVerify = time.Now().Sub(start)

		v.logger.WithFields(logrus.Fields{
			"store_size_before": before,
			"store_size_after":  after,
			"iteration":         iteration,
		}).Infof("completed re-verification iteration %d", iteration)

		if after <= 1000 || after >= before {
			break
		}
	}

	if v.MaxExpectedDowntime != 0 && timeToVerify > v.MaxExpectedDowntime {
		return fmt.Errorf("cutover stage verification will not complete within max downtime duration (took %s)", timeToVerify)
	}

	return nil
}

func (v *IterativeVerifier) iterateAllTables(mismatchedPaginationKeyFunc func(string, *TableSchema) error) error {
	pool := &WorkerPool{
		Concurrency: v.Concurrency,
		Process: func(tableIndex int) (interface{}, error) {
			table := v.Tables[tableIndex]

			if v.tableIsIgnored(table) {
				return nil, nil
			}

			err := v.iterateTableFingerprints(table, mismatchedPaginationKeyFunc)
			if err != nil {
				v.logger.WithError(err).WithField("table", table.String()).Error("error occured during table verification")
			}
			return nil, err
		},
	}

	_, err := pool.Run(len(v.Tables))

	return err
}

func (v *IterativeVerifier) iterateTableFingerprints(table *TableSchema, mismatchedPaginationKeyFunc func(string, *TableSchema) error) error {
	// The cursor will stop iterating when it cannot find anymore rows,
	// so it will not iterate until MaxPaginationKey.
	paginationColumns := table.GetPaginationColumns()
	minKey := MinPaginationKey(paginationColumns)
	maxKey := MaxPaginationKey(paginationColumns)

	cursor := v.CursorConfig.NewCursorWithoutRowLock(table, minKey, maxKey)

	// It only needs the PaginationKeys, not the entire row.
	columnsToSelect := make([]string, len(paginationColumns))
	for i, col := range paginationColumns {
		columnsToSelect[i] = fmt.Sprintf("`%s`", col.Name)
	}
	cursor.ColumnsToSelect = columnsToSelect

	return cursor.Each(func(batch *RowBatch) error {
		metrics.Count("RowEvent", int64(batch.Size()), []MetricTag{
			MetricTag{"table", table.Name},
			MetricTag{"source", "iterative_verifier_before_cutover"},
		}, 1.0)

		paginationKeys := make([]interface{}, 0, batch.Size())
		paginationKeyIndexes := batch.PaginationKeyIndexes()

		for _, rowData := range batch.Values() {
			if len(paginationColumns) == 1 {
				// Single column - use existing logic
				paginationColumn := paginationColumns[0]
				switch paginationColumn.Type {
				case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
					paginationKeyUint, err := rowData.GetUint64(paginationKeyIndexes[0])
					if err != nil {
						return err
					}
					paginationKeys = append(paginationKeys, paginationKeyUint)

				case schema.TYPE_BINARY, schema.TYPE_STRING:
					paginationKeyInterface := rowData[paginationKeyIndexes[0]]
					var paginationKeyBytes []byte
					switch v := paginationKeyInterface.(type) {
					case []byte:
						paginationKeyBytes = v
					case string:
						paginationKeyBytes = []byte(v)
					default:
						return fmt.Errorf("expected binary/string pagination key, got %T", paginationKeyInterface)
					}
					paginationKeys = append(paginationKeys, paginationKeyBytes)

				default:
					paginationKeyUint, err := rowData.GetUint64(paginationKeyIndexes[0])
					if err != nil {
						return err
					}
					paginationKeys = append(paginationKeys, paginationKeyUint)
				}
			} else {
				// Composite key - append as a string representation
				keys := make([]PaginationKey, len(paginationColumns))
				for i, paginationColumn := range paginationColumns {
					idx := paginationKeyIndexes[i]
					switch paginationColumn.Type {
					case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
						paginationKeyUint, err := rowData.GetUint64(idx)
						if err != nil {
							return err
						}
						keys[i] = NewUint64Key(paginationKeyUint)

					case schema.TYPE_BINARY, schema.TYPE_STRING:
						paginationKeyInterface := rowData[idx]
						var paginationKeyBytes []byte
						switch v := paginationKeyInterface.(type) {
						case []byte:
							paginationKeyBytes = v
						case string:
							paginationKeyBytes = []byte(v)
						default:
							return fmt.Errorf("expected binary/string pagination key, got %T", paginationKeyInterface)
						}
						keys[i] = NewBinaryKey(paginationKeyBytes)

					default:
						paginationKeyUint, err := rowData.GetUint64(idx)
						if err != nil {
							return err
						}
						keys[i] = NewUint64Key(paginationKeyUint)
					}
				}
				// Store as string for comparison with GetHashes results
				paginationKeys = append(paginationKeys, CompositeKey(keys).String())
			}
		}

		mismatchedPaginationKeys, err := v.compareFingerprints(paginationKeys, batch.TableSchema())
		if err != nil {
			v.logger.WithError(err).Errorf("failed to fingerprint table %s", batch.TableSchema().String())
			return err
		}

		if len(mismatchedPaginationKeys) > 0 {
			v.logger.WithFields(logrus.Fields{
				"table":                     batch.TableSchema().String(),
				"mismatched_paginationKeys": mismatchedPaginationKeys,
			}).Info("found mismatched rows")

			for _, paginationKey := range mismatchedPaginationKeys {
				err := mismatchedPaginationKeyFunc(paginationKey, batch.TableSchema())
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (v *IterativeVerifier) verifyStore(sourceTag string, additionalTags []MetricTag) (VerificationResult, error) {
	allBatches := v.reverifyStore.FlushAndBatchByTable(int(*v.CursorConfig.BatchSize))
	v.logger.WithField("batches", len(allBatches)).Debug("reverifying")

	if len(allBatches) == 0 {
		return NewCorrectVerificationResult(), nil
	}

	erroredOrFailed := errors.New("verification of store errored or failed")

	pool := &WorkerPool{
		Concurrency: v.Concurrency,
		Process: func(reverifyBatchIndex int) (interface{}, error) {
			reverifyBatch := allBatches[reverifyBatchIndex]
			table := v.TableSchemaCache.Get(reverifyBatch.Table.SchemaName, reverifyBatch.Table.TableName)

			tags := append([]MetricTag{
				MetricTag{"table", table.Name},
				MetricTag{"source", sourceTag},
			}, additionalTags...)

			metrics.Count("RowEvent", int64(len(reverifyBatch.PaginationKeys)), tags, 1.0)

			v.logger.WithFields(logrus.Fields{
				"table":               table.String(),
				"len(paginationKeys)": len(reverifyBatch.PaginationKeys),
			}).Debug("received paginationKey batch to reverify")

			verificationResult, mismatchedPaginationKeys, err := v.reverifyPaginationKeys(table, reverifyBatch.PaginationKeys)
			resultAndErr := verificationResultAndError{verificationResult, err}

			// If we haven't entered the cutover phase yet, then reverification failures
			// could have been caused by ongoing writes. We will just re-add the rows for
			// the cutover verification and ignore the failure at this point here.
			if err == nil && !v.beforeCutoverVerifyDone {
				for _, paginationKey := range mismatchedPaginationKeys {
					v.reverifyStore.Add(ReverifyEntry{PaginationKey: paginationKey, Table: table})
				}

				resultAndErr.Result = NewCorrectVerificationResult()
			}

			if resultAndErr.ErroredOrFailed() {
				if resultAndErr.Error != nil {
					v.logger.WithError(resultAndErr.Error).Error("error occured in reverification")
				} else {
					v.logger.Errorf("failed reverification: %s", resultAndErr.Result.Message)
				}

				return resultAndErr, erroredOrFailed
			}

			return resultAndErr, nil
		},
	}

	results, _ := pool.Run(len(allBatches))

	var result VerificationResult
	var err error
	for i := 0; i < v.Concurrency; i++ {
		if results[i] == nil {
			// This means the worker pool exited early and another goroutine
			// must have returned an error.
			continue
		}

		resultAndErr := results[i].(verificationResultAndError)
		result = resultAndErr.Result
		err = resultAndErr.Error

		if resultAndErr.ErroredOrFailed() {
			break
		}
	}

	return result, err
}

func (v *IterativeVerifier) reverifyPaginationKeys(table *TableSchema, paginationKeys []interface{}) (VerificationResult, []string, error) {
	mismatchedPaginationKeys, err := v.compareFingerprints(paginationKeys, table)
	if err != nil {
		return VerificationResult{}, mismatchedPaginationKeys, err
	}

	if len(mismatchedPaginationKeys) == 0 {
		return NewCorrectVerificationResult(), mismatchedPaginationKeys, nil
	}

	return VerificationResult{
		DataCorrect:     false,
		Message:         fmt.Sprintf("verification failed on table: %s for paginationKeys: %s", table.String(), strings.Join(mismatchedPaginationKeys, ",")),
		IncorrectTables: []string{table.String()},
	}, mismatchedPaginationKeys, nil
}

func (v *IterativeVerifier) binlogEventListener(evs []DMLEvent) error {
	if v.verifyDuringCutoverStarted.Get() {
		return fmt.Errorf("cutover has started but received binlog event!")
	}

	for _, ev := range evs {
		if v.tableIsIgnored(ev.TableSchema()) {
			continue
		}

		paginationKey, err := ev.PaginationKey()
		if err != nil {
			return err
		}

		v.reverifyStore.Add(ReverifyEntry{PaginationKey: paginationKey, Table: ev.TableSchema()})
	}

	return nil
}

func (v *IterativeVerifier) tableIsIgnored(table *TableSchema) bool {
	for _, ignored := range v.IgnoredTables {
		if table.Name == ignored {
			return true
		}
	}

	return false
}

func (v *IterativeVerifier) columnsToVerify(table *TableSchema) []schema.TableColumn {
	ignoredColsSet, containsIgnoredColumns := v.IgnoredColumns[table.Name]
	if !containsIgnoredColumns {
		return table.Columns
	}

	var columns []schema.TableColumn
	for _, column := range table.Columns {
		if _, isIgnored := ignoredColsSet[column.Name]; !isIgnored {
			columns = append(columns, column)
		}
	}

	return columns
}

func (v *IterativeVerifier) compareFingerprints(paginationKeys []interface{}, table *TableSchema) ([]string, error) {
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

	var sourceHashes map[string][]byte
	var sourceErr error
	go func() {
		defer wg.Done()
		sourceErr = WithRetries(5, 0, v.logger, "get fingerprints from source db", func() (err error) {
			// Pass deprecated single column name for backward compatibility (unused in GetHashes now)
			sourceHashes, err = v.GetHashes(v.SourceDB, table.Schema, table.Name, "", v.columnsToVerify(table), paginationKeys)
			return
		})
	}()

	var targetHashes map[string][]byte
	var targetErr error
	go func() {
		defer wg.Done()
		targetErr = WithRetries(5, 0, v.logger, "get fingerprints from target db", func() (err error) {
			// Pass deprecated single column name for backward compatibility (unused in GetHashes now)
			targetHashes, err = v.GetHashes(v.TargetDB, targetDb, targetTable, "", v.columnsToVerify(table), paginationKeys)
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

	mismatches := compareHashes(sourceHashes, targetHashes)
	if len(mismatches) > 0 && v.CompressionVerifier != nil && v.CompressionVerifier.IsCompressedTable(table.Name) {
		return v.compareCompressedHashes(targetDb, targetTable, table, paginationKeys)
	}

	return mismatches, nil
}

func (v *IterativeVerifier) compareCompressedHashes(targetDb, targetTable string, table *TableSchema, paginationKeys []interface{}) ([]string, error) {
	// Pass empty string for deprecated paginationKeyColumn parameter (CompressionVerifier will get it from TableSchemaCache)
	sourceHashes, err := v.CompressionVerifier.GetCompressedHashes(v.SourceDB, table.Schema, table.Name, "", v.columnsToVerify(table), paginationKeys)
	if err != nil {
		return nil, err
	}

	targetHashes, err := v.CompressionVerifier.GetCompressedHashes(v.TargetDB, targetDb, targetTable, "", v.columnsToVerify(table), paginationKeys)
	if err != nil {
		return nil, err
	}

	return compareHashes(sourceHashes, targetHashes), nil
}

func compareHashes(source, target map[string][]byte) []string {
	mismatchSet := map[string]struct{}{}

	for paginationKey, targetHash := range target {
		sourceHash, exists := source[paginationKey]
		if !bytes.Equal(sourceHash, targetHash) || !exists {
			mismatchSet[paginationKey] = struct{}{}
		}
	}

	for paginationKey, sourceHash := range source {
		targetHash, exists := target[paginationKey]
		if !bytes.Equal(sourceHash, targetHash) || !exists {
			mismatchSet[paginationKey] = struct{}{}
		}
	}

	mismatches := make([]string, 0, len(mismatchSet))
	for mismatch, _ := range mismatchSet {
		mismatches = append(mismatches, mismatch)
	}

	return mismatches
}

func GetMd5HashesSql(schemaName, table string, paginationKeyColumns []*schema.TableColumn, columns []schema.TableColumn, paginationKeys []interface{}) (string, []interface{}, error) {
	builder := rowMd5Selector(columns, paginationKeyColumns).
		From(QuotedTableNameFromString(schemaName, table))

	if len(paginationKeyColumns) == 1 {
		// Single column WHERE clause
		quotedPaginationKey := QuoteField(paginationKeyColumns[0].Name)
		builder = builder.Where(sq.Eq{quotedPaginationKey: paginationKeys})
		builder = builder.OrderBy(quotedPaginationKey)

		return builder.ToSql()
	}

	// Composite key WHERE clause: (col1, col2) IN ((?, ?), (?, ?), ...)
	quotedPKCols := make([]string, len(paginationKeyColumns))
	for i, col := range paginationKeyColumns {
		quotedPKCols[i] = QuoteField(col.Name)
	}
	tuple := fmt.Sprintf("(%s)", strings.Join(quotedPKCols, ", "))

	// Build placeholder tuples for each pagination key string
	placeholderTuples := make([]string, len(paginationKeys))
	args := make([]interface{}, 0, len(paginationKeys)*len(paginationKeyColumns))

	for i, pkInterface := range paginationKeys {
		pkStr, ok := pkInterface.(string)
		if !ok {
			return "", nil, fmt.Errorf("expected string pagination key for composite key, got %T", pkInterface)
		}

		// Parse the composite key string (comma-separated)
		parts := strings.Split(pkStr, ",")
		if len(parts) != len(paginationKeyColumns) {
			return "", nil, fmt.Errorf("pagination key has %d parts but expected %d", len(parts), len(paginationKeyColumns))
		}

		placeholders := make([]string, len(parts))
		for j, part := range parts {
			placeholders[j] = "?"
			// Convert string representation back to appropriate type
			col := paginationKeyColumns[j]
			switch col.Type {
			case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
				val, err := strconv.ParseUint(part, 10, 64)
				if err != nil {
					return "", nil, fmt.Errorf("failed to parse pagination key part %q as uint64: %w", part, err)
				}
				args = append(args, val)
			case schema.TYPE_BINARY, schema.TYPE_STRING:
				// For binary keys, the string is hex-encoded
				decoded, err := hex.DecodeString(part)
				if err != nil {
					return "", nil, fmt.Errorf("failed to decode pagination key part %q: %w", part, err)
				}
				args = append(args, decoded)
			default:
				val, err := strconv.ParseUint(part, 10, 64)
				if err != nil {
					return "", nil, fmt.Errorf("failed to parse pagination key part %q: %w", part, err)
				}
				args = append(args, val)
			}
		}
		placeholderTuples[i] = fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	}

	whereClause := fmt.Sprintf("%s IN (%s)", tuple, strings.Join(placeholderTuples, ", "))
	builder = builder.Where(whereClause, args...)
	builder = builder.OrderBy(strings.Join(quotedPKCols, ", "))

	return builder.ToSql()
}

func rowMd5Selector(columns []schema.TableColumn, paginationKeyColumns []*schema.TableColumn) sq.SelectBuilder {
	// Select all pagination key columns
	selectParts := make([]string, len(paginationKeyColumns))
	for i, col := range paginationKeyColumns {
		selectParts[i] = QuoteField(col.Name)
	}

	hashStrs := make([]string, len(columns))
	for idx, column := range columns {
		quotedCol := normalizeAndQuoteColumn(column)
		hashStrs[idx] = fmt.Sprintf("MD5(COALESCE(%s, 'NULL'))", quotedCol)
	}

	return sq.Select(fmt.Sprintf(
		"%s, MD5(CONCAT(%s)) AS row_fingerprint",
		strings.Join(selectParts, ", "),
		strings.Join(hashStrs, ","),
	))
}

func normalizeAndQuoteColumn(column schema.TableColumn) (quoted string) {
	quoted = QuoteField(column.Name)
	if column.Type == schema.TYPE_FLOAT {
		quoted = fmt.Sprintf("(if (%s = '-0', 0, %s))", quoted, quoted)
	}
	return
}
