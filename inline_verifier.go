package ghostferry

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/sirupsen/logrus"
)

// This struct is very similar to ReverifyStore, but it is more optimized
// for serialization into JSON.
//
// TODO: remove IterativeVerifier and remove this comment.
type BinlogVerifyStore struct {
	EmitLogPerRowsAdded uint64

	mutex *sync.Mutex
	store map[string]map[string]map[uint64]struct{} // db => table => (pk set)

	// The total number of rows added to the reverify store, ever.  Does not
	// include the rows added in the interrupted run if the present run is a
	// resuming one. This is only used for emitting metrics.
	totalRowCount   uint64
	currentRowCount uint64 // The number of rows in store currently.
}

type BinlogVerifySerializedStore struct {
	Store    map[string]map[string]map[uint64]struct{}
	RowCount uint64
}

type BinlogVerifyBatch struct {
	SchemaName string
	TableName  string
	Pks        []uint64
}

func NewBinlogVerifyStore() *BinlogVerifyStore {
	return &BinlogVerifyStore{
		EmitLogPerRowsAdded: uint64(10000), // TODO: make this configurable
		mutex:               &sync.Mutex{},
		store:               make(map[string]map[string]map[uint64]struct{}),
		totalRowCount:       uint64(0),
		currentRowCount:     uint64(0),
	}
}

func (s *BinlogVerifyStore) Add(table *TableSchema, pk uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists := s.store[table.Schema]
	if !exists {
		s.store[table.Schema] = make(map[string]map[uint64]struct{})
	}

	_, exists = s.store[table.Schema][table.Name]
	if !exists {
		s.store[table.Schema][table.Name] = make(map[uint64]struct{})
	}

	_, exists = s.store[table.Schema][table.Name][pk]
	if !exists {
		s.store[table.Schema][table.Name][pk] = struct{}{}
		s.totalRowCount++
		s.currentRowCount++

		if s.totalRowCount%s.EmitLogPerRowsAdded == 0 {
			metrics.Gauge("inline_verifier_current_rows", float64(s.currentRowCount), []MetricTag{}, 1.0)
			metrics.Gauge("inline_verifier_total_rows", float64(s.totalRowCount), []MetricTag{}, 1.0)
			logrus.WithFields(logrus.Fields{
				"tag":         "binlog_verify_store",
				"currentRows": s.currentRowCount,
				"totalRows":   s.totalRowCount,
			}).Debug("current rows in BinlogVerifyStore")
		}
	}
}

func (s *BinlogVerifyStore) FlushAndBatchByTable(batchsize int) []BinlogVerifyBatch {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	batches := make([]BinlogVerifyBatch, 0)
	for schemaName, _ := range s.store {
		for tableName, pkSet := range s.store[schemaName] {
			pkBatch := make([]uint64, 0, batchsize)

			for pk, _ := range pkSet {
				pkBatch = append(pkBatch, pk)
				if len(pkBatch) >= batchsize {
					batches = append(batches, BinlogVerifyBatch{
						SchemaName: schemaName,
						TableName:  tableName,
						Pks:        pkBatch,
					})
					pkBatch = make([]uint64, 0, batchsize)
				}
			}

			if len(pkBatch) > 0 {
				batches = append(batches, BinlogVerifyBatch{
					SchemaName: schemaName,
					TableName:  tableName,
					Pks:        pkBatch,
				})
			}
		}
	}

	s.store = make(map[string]map[string]map[uint64]struct{})
	s.currentRowCount = 0
	return batches
}

type InlineVerifier struct {
	SourceDB                   *sql.DB
	TargetDB                   *sql.DB
	DatabaseRewrites           map[string]string
	TableRewrites              map[string]string
	TableSchemaCache           TableSchemaCache
	BatchSize                  int
	VerifyBinlogEventsInterval time.Duration
	MaxExpectedDowntime        time.Duration

	ErrorHandler ErrorHandler

	reverifyStore              *BinlogVerifyStore
	verifyDuringCutoverStarted AtomicBoolean

	sourceStmtCache *StmtCache
	targetStmtCache *StmtCache
	logger          *logrus.Entry
}

func (v *InlineVerifier) StartInBackground() error {
	// not needed?
	return nil
}

func (v *InlineVerifier) Wait() {
	// not needed?
}

func (v *InlineVerifier) Result() (VerificationResultAndStatus, error) {
	// not implemented for now
	return VerificationResultAndStatus{}, nil
}

func (v *InlineVerifier) CheckFingerprintInline(tx *sql.Tx, targetSchema, targetTable string, sourceBatch *RowBatch) ([]uint64, error) {
	table := sourceBatch.TableSchema()

	pks := make([]uint64, len(sourceBatch.Values()))
	for i, row := range sourceBatch.Values() {
		pk, err := row.GetUint64(sourceBatch.PkIndex())
		if err != nil {
			return nil, err
		}

		pks[i] = pk
	}

	// Fetch target data
	targetFingerprints, targetDecompressedData, err := v.getFingerprintDataFromTargetDb(targetSchema, targetTable, tx, table, pks)
	if err != nil {
		return nil, err
	}

	// Fetch source data
	sourceFingerprints := sourceBatch.Fingerprints()
	sourceDecompressedData := make(map[uint64]map[string][]byte)

	for _, rowData := range sourceBatch.Values() {
		pk, err := rowData.GetUint64(sourceBatch.PkIndex())
		if err != nil {
			return nil, err
		}

		sourceDecompressedData[pk] = make(map[string][]byte)
		for idx, col := range table.Columns {
			var compressedData []byte
			var ok bool
			if _, ok = table.CompressedColumns[col.Name]; !ok {
				continue
			}

			compressedData, ok = rowData[idx].([]byte)
			if !ok {
				return nil, fmt.Errorf("cannot convert column %v to []byte", col.Name)
			}

			sourceDecompressedData[pk][col.Name], err = v.decompressData(table, col.Name, compressedData)
		}
	}

	return v.compareHashesAndData(sourceFingerprints, targetFingerprints, sourceDecompressedData, targetDecompressedData), nil
}

func (v *InlineVerifier) PeriodicallyVerifyBinlogEvents(ctx context.Context) {
	v.logger.Info("starting periodic reverifier")
	ticker := time.NewTicker(v.VerifyBinlogEventsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, mismatches, err := v.verifyAllEventsInStore()
			if err != nil {
				v.ErrorHandler.Fatal("inline_verifier", err)
			}

			v.readdMismatchedPKsToBeVerifiedAgain(mismatches)

			v.logger.WithFields(logrus.Fields{
				"remainingRowCount": v.reverifyStore.currentRowCount,
			}).Debug("reverified")
		case <-ctx.Done():
			v.logger.Info("shutdown periodic reverifier")
			return
		}
	}

}

func (v *InlineVerifier) VerifyBeforeCutover() error {
	var timeToVerify time.Duration
	// Iterate until the reverify queue is small enough
	// Maximum 30 iterations.
	for i := 0; i < 30; i++ {
		before := v.reverifyStore.currentRowCount
		start := time.Now()

		_, mismatches, err := v.verifyAllEventsInStore()
		if err != nil {
			return err
		}

		v.readdMismatchedPKsToBeVerifiedAgain(mismatches)
		after := v.reverifyStore.currentRowCount
		timeToVerify = time.Now().Sub(start)

		v.logger.WithFields(logrus.Fields{
			"store_size_before": before,
			"store_size_after":  after,
			"iteration":         i,
		}).Infof("completed re-verification iteration %d", i)

		if after <= 1000 || after >= before {
			break
		}
	}

	if v.MaxExpectedDowntime != 0 && timeToVerify > v.MaxExpectedDowntime {
		return fmt.Errorf("cutover stage verification will not complete within max downtime duration (took %s)", timeToVerify)
	}

	return nil
}

func (v *InlineVerifier) VerifyDuringCutover() (VerificationResult, error) {
	v.verifyDuringCutoverStarted.Set(true)
	mismatchFound, mismatches, err := v.verifyAllEventsInStore()
	if err != nil {
		v.logger.WithError(err).Error("failed to VerifyDuringCutover")
		return VerificationResult{}, err
	}

	if !mismatchFound {
		return VerificationResult{
			DataCorrect: true,
		}, nil
	}

	// Build error message for display
	var messageBuf bytes.Buffer
	messageBuf.WriteString("cutover verification failed for: ")
	incorrectTables := make([]string, 0)
	for schemaName, _ := range mismatches {
		for tableName, pks := range mismatches[schemaName] {
			tableName = fmt.Sprintf("%s.%s", schemaName, tableName)
			incorrectTables = append(incorrectTables, tableName)

			messageBuf.WriteString(tableName)
			messageBuf.WriteString(" [pks: ")
			for _, pk := range pks {
				messageBuf.WriteString(strconv.FormatUint(pk, 10))
				messageBuf.WriteString(" ")
			}
			messageBuf.WriteString("] ")
		}
	}

	message := messageBuf.String()
	v.logger.WithField("incorrect_tables", incorrectTables).Error(message)

	return VerificationResult{
		DataCorrect:     false,
		Message:         messageBuf.String(),
		IncorrectTables: incorrectTables,
	}, nil
}

func (v *InlineVerifier) getFingerprintDataFromSourceDb(schemaName, tableName string, tx *sql.Tx, table *TableSchema, pks []uint64) (map[uint64][]byte, map[uint64]map[string][]byte, error) {
	return v.getFingerprintDataFromDb(v.SourceDB, v.sourceStmtCache, schemaName, tableName, tx, table, pks)
}

func (v *InlineVerifier) getFingerprintDataFromTargetDb(schemaName, tableName string, tx *sql.Tx, table *TableSchema, pks []uint64) (map[uint64][]byte, map[uint64]map[string][]byte, error) {
	return v.getFingerprintDataFromDb(v.TargetDB, v.targetStmtCache, schemaName, tableName, tx, table, pks)
}

func (v *InlineVerifier) getFingerprintDataFromDb(db *sql.DB, stmtCache *StmtCache, schemaName, tableName string, tx *sql.Tx, table *TableSchema, pks []uint64) (map[uint64][]byte, map[uint64]map[string][]byte, error) {
	fingerprintQuery := table.FingerprintQuery(schemaName, tableName, len(pks))
	fingerprintStmt, err := stmtCache.StmtFor(db, fingerprintQuery)
	if err != nil {
		return nil, nil, err
	}

	if tx != nil {
		fingerprintStmt = tx.Stmt(fingerprintStmt)
	}

	args := make([]interface{}, len(pks))
	for i, pk := range pks {
		args[i] = pk
	}

	rows, err := fingerprintStmt.Query(args...)

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	fingerprints := make(map[uint64][]byte)                // pk -> fingerprint
	decompressedData := make(map[uint64]map[string][]byte) // pk -> columnName -> decompressedData

	for rows.Next() {
		rowData, err := ScanByteRow(rows, len(columns))
		if err != nil {
			return nil, nil, err
		}

		pk, err := strconv.ParseUint(string(rowData[0]), 10, 64)
		if err != nil {
			return nil, nil, err
		}

		fingerprints[pk] = rowData[1]
		decompressedData[pk] = make(map[string][]byte)

		// Note that the FingerprintQuery returns the columns: pk, fingerprint,
		// compressedData1, compressedData2, ...
		// If there are no compressed data, only 2 columns are returned and this
		// loop will be skipped.
		for i := 2; i < len(columns); i++ {
			decompressedData[pk][columns[i]], err = v.decompressData(table, columns[i], rowData[i])
			if err != nil {
				return nil, nil, err
			}
		}
	}

	return fingerprints, decompressedData, nil
}

func (v *InlineVerifier) decompressData(table *TableSchema, column string, compressed []byte) ([]byte, error) {
	var decompressed []byte
	algorithm, isCompressed := table.CompressedColumns[column]
	if !isCompressed {
		return nil, fmt.Errorf("%v is not a compressed column", column)
	}

	switch strings.ToUpper(algorithm) {
	case CompressionSnappy:
		return snappy.Decode(decompressed, compressed)
	default:
		return nil, UnsupportedCompressionError{
			table:     table.String(),
			column:    column,
			algorithm: algorithm,
		}
	}
}

func (v *InlineVerifier) compareHashes(source, target map[uint64][]byte) map[uint64]struct{} {
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

	return mismatchSet
}

func (v *InlineVerifier) compareDecompressedData(source, target map[uint64]map[string][]byte) map[uint64]struct{} {
	mismatchSet := map[uint64]struct{}{}

	for pk, targetDecompressedColumns := range target {
		sourceDecompressedColumns, exists := source[pk]
		if !exists {
			mismatchSet[pk] = struct{}{}
			continue
		}

		for colName, targetData := range targetDecompressedColumns {
			sourceData, exists := sourceDecompressedColumns[colName]
			if !exists || !bytes.Equal(sourceData, targetData) {
				mismatchSet[pk] = struct{}{}
				break // no need to compare other columns
			}
		}
	}

	for pk, sourceDecompressedColumns := range source {
		targetDecompressedColumns, exists := target[pk]
		if !exists {
			mismatchSet[pk] = struct{}{}
			continue
		}

		for colName, sourceData := range sourceDecompressedColumns {
			targetData, exists := targetDecompressedColumns[colName]
			if !exists || !bytes.Equal(sourceData, targetData) {
				mismatchSet[pk] = struct{}{}
				break
			}
		}
	}

	return mismatchSet
}

func (v *InlineVerifier) compareHashesAndData(sourceHashes, targetHashes map[uint64][]byte, sourceData, targetData map[uint64]map[string][]byte) []uint64 {
	mismatches := v.compareHashes(sourceHashes, targetHashes)
	compressedMismatch := v.compareDecompressedData(sourceData, targetData)
	for pk, _ := range compressedMismatch {
		mismatches[pk] = struct{}{}
	}

	mismatchList := make([]uint64, 0, len(mismatches))

	for pk, _ := range mismatches {
		mismatchList = append(mismatchList, pk)
	}

	return mismatchList
}

func (v *InlineVerifier) binlogEventListener(evs []DMLEvent) error {
	if v.verifyDuringCutoverStarted.Get() {
		return fmt.Errorf("cutover has started but received binlog event!")
	}

	for _, ev := range evs {
		pk, err := ev.PK()
		if err != nil {
			return err
		}

		v.reverifyStore.Add(ev.TableSchema(), pk)
	}

	return nil
}

func (v *InlineVerifier) readdMismatchedPKsToBeVerifiedAgain(mismatches map[string]map[string][]uint64) {
	for schemaName, _ := range mismatches {
		for tableName, pks := range mismatches[schemaName] {
			table := v.TableSchemaCache.Get(schemaName, tableName)
			for _, pk := range pks {
				v.reverifyStore.Add(table, pk)
			}
		}
	}
}

// Returns mismatches in the form of db -> table -> pks
func (v *InlineVerifier) verifyAllEventsInStore() (bool, map[string]map[string][]uint64, error) {
	mismatchFound := false
	mismatches := make(map[string]map[string][]uint64)
	allBatches := v.reverifyStore.FlushAndBatchByTable(v.BatchSize)

	if len(allBatches) == 0 {
		return mismatchFound, mismatches, nil
	}

	v.logger.WithField("batches", len(allBatches)).Debug("reverifying")

	for _, batch := range allBatches {
		if _, exists := mismatches[batch.SchemaName]; !exists {
			mismatches[batch.SchemaName] = make(map[string][]uint64)
		}

		if _, exists := mismatches[batch.SchemaName][batch.TableName]; !exists {
			mismatches[batch.SchemaName][batch.TableName] = make([]uint64, 0)
		}

		batchMismatches, err := v.verifyBinlogBatch(batch)
		if err != nil {
			return false, nil, err
		}

		if len(batchMismatches) > 0 {
			mismatchFound = true
			mismatches[batch.SchemaName][batch.TableName] = append(mismatches[batch.SchemaName][batch.TableName], batchMismatches...)
		}
	}

	return mismatchFound, mismatches, nil
}

// Returns a list of mismatched PKs.
// Since the mismatches gets re-added to the reverify store, this must return
// a union of mismatches of fingerprints and mismatches due to decompressed
// data.
func (v *InlineVerifier) verifyBinlogBatch(batch BinlogVerifyBatch) ([]uint64, error) {
	targetSchema := batch.SchemaName
	if targetSchemaName, exists := v.DatabaseRewrites[targetSchema]; exists {
		targetSchema = targetSchemaName
	}

	targetTable := batch.TableName
	if targetTableName, exists := v.TableRewrites[targetTable]; exists {
		targetTable = targetTableName
	}

	sourceTableSchema := v.TableSchemaCache.Get(batch.SchemaName, batch.TableName)
	if sourceTableSchema == nil {
		return []uint64{}, fmt.Errorf("programming error? %s.%s is not found in TableSchemaCache but is being reverified", batch.SchemaName, batch.TableName)
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)

	var sourceFingerprints map[uint64][]byte
	var sourceDecompressedData map[uint64]map[string][]byte
	var sourceErr error
	go func() {
		defer wg.Done()
		sourceErr = WithRetries(5, 0, v.logger, "get fingerprints from source db", func() (err error) {
			sourceFingerprints, sourceDecompressedData, err = v.getFingerprintDataFromSourceDb(
				batch.SchemaName, batch.TableName,
				nil, // No transaction
				sourceTableSchema,
				batch.Pks,
			)
			return
		})
	}()

	var targetFingerprints map[uint64][]byte
	var targetDecompressedData map[uint64]map[string][]byte
	var targetErr error
	go func() {
		defer wg.Done()
		targetErr = WithRetries(5, 0, v.logger, "get fingerprints from target db", func() (err error) {
			targetFingerprints, targetDecompressedData, err = v.getFingerprintDataFromTargetDb(
				targetSchema, targetTable,
				nil, // No transaction
				sourceTableSchema,
				batch.Pks,
			)
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

	return v.compareHashesAndData(sourceFingerprints, targetFingerprints, sourceDecompressedData, targetDecompressedData), nil
}
