package ghostferry

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

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
	// db => table => paginationKey => number of times it changed.
	//
	// We need to store the number of times the row has changed because of the
	// following series of events:
	//
	// 1. A row changed.
	// 2. The row gets verified in a batch.
	// 3. Still while verifying the same batch, the row verified in step 2 gets
	//    changed on the source databse. This should cause the row to be requeued
	//    to into the BinlogVerifyStore.
	// 4. The batch verification finishes and we need to delete the rows in that
	//    batch from the BinlogVerifyStore. But we cannot lose the row verified
	//    and subsequently changed in step 2 and step 3.
	//
	// Storing the number of times the row has changed means that step 4 will
	// simply decrement this number.
	//
	// We may waste some CPU cycles by verify a row multiple times unnecessarily,
	// but at least this approach is correct for now without major rework of the
	// BinlogVerifyStore data structure and any code that manipulates this store.
	store BinlogVerifySerializedStore

	// The total number of rows added to the reverify store, ever.  Does not
	// include the rows added in the interrupted run if the present run is a
	// resuming one. This is only used for emitting metrics.
	totalRowCount   uint64
	currentRowCount uint64 // The number of rows in store currently.
}

type BinlogVerifySerializedStore map[string]map[string]map[uint64]int

func (s BinlogVerifySerializedStore) RowCount() uint64 {
	var v uint64 = 0
	for _, dbStore := range s {
		for _, tableStore := range dbStore {
			for _, count := range tableStore {
				v += uint64(count)
			}
		}
	}
	return v
}

func (s BinlogVerifySerializedStore) Copy() BinlogVerifySerializedStore {
	copyS := make(BinlogVerifySerializedStore)

	for db, _ := range s {
		copyS[db] = make(map[string]map[uint64]int)
		for table, _ := range s[db] {
			copyS[db][table] = make(map[uint64]int)
			for paginationKey, count := range s[db][table] {
				copyS[db][table][paginationKey] = count
			}
		}
	}

	return copyS
}

type BinlogVerifyBatch struct {
	SchemaName     string
	TableName      string
	PaginationKeys []uint64
}

func NewBinlogVerifyStore() *BinlogVerifyStore {
	return &BinlogVerifyStore{
		EmitLogPerRowsAdded: uint64(10000), // TODO: make this configurable
		mutex:               &sync.Mutex{},
		store:               make(map[string]map[string]map[uint64]int),
		totalRowCount:       uint64(0),
		currentRowCount:     uint64(0),
	}
}

func NewBinlogVerifyStoreFromSerialized(serialized BinlogVerifySerializedStore) *BinlogVerifyStore {
	s := NewBinlogVerifyStore()

	s.store = serialized
	s.currentRowCount = serialized.RowCount()

	s.totalRowCount = s.currentRowCount
	return s
}

func (s *BinlogVerifyStore) Add(table *TableSchema, paginationKey uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists := s.store[table.Schema]
	if !exists {
		s.store[table.Schema] = make(map[string]map[uint64]int)
	}

	_, exists = s.store[table.Schema][table.Name]
	if !exists {
		s.store[table.Schema][table.Name] = make(map[uint64]int)
	}

	_, exists = s.store[table.Schema][table.Name][paginationKey]
	if !exists {
		s.store[table.Schema][table.Name][paginationKey] = 0
	}

	s.store[table.Schema][table.Name][paginationKey]++
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

func (s *BinlogVerifyStore) RemoveVerifiedBatch(batch BinlogVerifyBatch) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	dbStore, exists := s.store[batch.SchemaName]
	if !exists {
		return
	}

	tableStore, exists := dbStore[batch.TableName]
	if !exists {
		return
	}

	for _, paginationKey := range batch.PaginationKeys {
		if _, exists = tableStore[paginationKey]; exists {
			if tableStore[paginationKey] <= 1 {
				// Even though this doesn't save as RAM, it will save space on the
				// serialized output.
				delete(tableStore, paginationKey)
			} else {
				tableStore[paginationKey]--
			}
			s.currentRowCount--
		}
	}
}

func (s *BinlogVerifyStore) Batches(batchsize int) []BinlogVerifyBatch {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	batches := make([]BinlogVerifyBatch, 0)
	for schemaName, _ := range s.store {
		for tableName, paginationKeySet := range s.store[schemaName] {
			paginationKeyBatch := make([]uint64, 0, batchsize)

			for paginationKey, _ := range paginationKeySet {
				paginationKeyBatch = append(paginationKeyBatch, paginationKey)
				if len(paginationKeyBatch) >= batchsize {
					batches = append(batches, BinlogVerifyBatch{
						SchemaName:     schemaName,
						TableName:      tableName,
						PaginationKeys: paginationKeyBatch,
					})
					paginationKeyBatch = make([]uint64, 0, batchsize)
				}
			}

			if len(paginationKeyBatch) > 0 {
				batches = append(batches, BinlogVerifyBatch{
					SchemaName:     schemaName,
					TableName:      tableName,
					PaginationKeys: paginationKeyBatch,
				})
			}
		}
	}

	return batches
}

func (s *BinlogVerifyStore) Serialize() BinlogVerifySerializedStore {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.store.Copy()
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

	StateTracker *StateTracker
	ErrorHandler ErrorHandler

	reverifyStore              *BinlogVerifyStore
	verifyDuringCutoverStarted AtomicBoolean
	cutoverCompleted           AtomicBoolean

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

	paginationKeys := make([]uint64, len(sourceBatch.Values()))
	for i, row := range sourceBatch.Values() {
		paginationKey, err := row.GetUint64(sourceBatch.PaginationKeyIndex())
		if err != nil {
			return nil, err
		}

		paginationKeys[i] = paginationKey
	}

	// Fetch target data
	targetFingerprints, targetDecompressedData, err := v.getFingerprintDataFromTargetDb(targetSchema, targetTable, tx, table, paginationKeys)
	if err != nil {
		return nil, err
	}

	// Fetch source data
	sourceFingerprints := sourceBatch.Fingerprints()
	sourceDecompressedData := make(map[uint64]map[string][]byte)

	for _, rowData := range sourceBatch.Values() {
		paginationKey, err := rowData.GetUint64(sourceBatch.PaginationKeyIndex())
		if err != nil {
			return nil, err
		}

		sourceDecompressedData[paginationKey] = make(map[string][]byte)
		for idx, col := range table.Columns {
			var compressedData []byte
			var ok bool
			if _, ok = table.CompressedColumnsForVerification[col.Name]; !ok {
				continue
			}

			compressedData, ok = rowData[idx].([]byte)
			if !ok {
				return nil, fmt.Errorf("cannot convert column %v to []byte", col.Name)
			}

			sourceDecompressedData[paginationKey][col.Name], err = v.decompressData(table, col.Name, compressedData)
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

			v.readdMismatchedPaginationKeysToBeVerifiedAgain(mismatches)

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

		v.readdMismatchedPaginationKeysToBeVerifiedAgain(mismatches)
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
	defer v.cutoverCompleted.Set(true)

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
		for tableName, paginationKeys := range mismatches[schemaName] {
			tableName = fmt.Sprintf("%s.%s", schemaName, tableName)
			incorrectTables = append(incorrectTables, tableName)

			messageBuf.WriteString(tableName)
			messageBuf.WriteString(" [paginationKeys: ")
			for _, paginationKey := range paginationKeys {
				messageBuf.WriteString(strconv.FormatUint(paginationKey, 10))
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

func (v *InlineVerifier) getFingerprintDataFromSourceDb(schemaName, tableName string, tx *sql.Tx, table *TableSchema, paginationKeys []uint64) (map[uint64][]byte, map[uint64]map[string][]byte, error) {
	return v.getFingerprintDataFromDb(v.SourceDB, v.sourceStmtCache, schemaName, tableName, tx, table, paginationKeys)
}

func (v *InlineVerifier) getFingerprintDataFromTargetDb(schemaName, tableName string, tx *sql.Tx, table *TableSchema, paginationKeys []uint64) (map[uint64][]byte, map[uint64]map[string][]byte, error) {
	return v.getFingerprintDataFromDb(v.TargetDB, v.targetStmtCache, schemaName, tableName, tx, table, paginationKeys)
}

func (v *InlineVerifier) getFingerprintDataFromDb(db *sql.DB, stmtCache *StmtCache, schemaName, tableName string, tx *sql.Tx, table *TableSchema, paginationKeys []uint64) (map[uint64][]byte, map[uint64]map[string][]byte, error) {
	fingerprintQuery := table.FingerprintQuery(schemaName, tableName, len(paginationKeys))
	fingerprintStmt, err := stmtCache.StmtFor(db, fingerprintQuery)
	if err != nil {
		return nil, nil, err
	}

	if tx != nil {
		fingerprintStmt = tx.Stmt(fingerprintStmt)
	}

	args := make([]interface{}, len(paginationKeys))
	for i, paginationKey := range paginationKeys {
		args[i] = paginationKey
	}

	rows, err := fingerprintStmt.Query(args...)

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	fingerprints := make(map[uint64][]byte)                // paginationKey -> fingerprint
	decompressedData := make(map[uint64]map[string][]byte) // paginationKey -> columnName -> decompressedData

	for rows.Next() {
		rowData, err := ScanByteRow(rows, len(columns))
		if err != nil {
			return nil, nil, err
		}

		paginationKey, err := strconv.ParseUint(string(rowData[0]), 10, 64)
		if err != nil {
			return nil, nil, err
		}

		fingerprints[paginationKey] = rowData[1]
		decompressedData[paginationKey] = make(map[string][]byte)

		// Note that the FingerprintQuery returns the columns: paginationKey, fingerprint,
		// compressedData1, compressedData2, ...
		// If there are no compressed data, only 2 columns are returned and this
		// loop will be skipped.
		for i := 2; i < len(columns); i++ {
			decompressedData[paginationKey][columns[i]], err = v.decompressData(table, columns[i], rowData[i])
			if err != nil {
				return nil, nil, err
			}
		}
	}

	return fingerprints, decompressedData, nil
}

func (v *InlineVerifier) decompressData(table *TableSchema, column string, compressed []byte) ([]byte, error) {
	var decompressed []byte
	algorithm, isCompressed := table.CompressedColumnsForVerification[column]
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

	return mismatchSet
}

func (v *InlineVerifier) compareDecompressedData(source, target map[uint64]map[string][]byte) map[uint64]struct{} {
	mismatchSet := map[uint64]struct{}{}

	for paginationKey, targetDecompressedColumns := range target {
		sourceDecompressedColumns, exists := source[paginationKey]
		if !exists {
			mismatchSet[paginationKey] = struct{}{}
			continue
		}

		for colName, targetData := range targetDecompressedColumns {
			sourceData, exists := sourceDecompressedColumns[colName]
			if !exists || !bytes.Equal(sourceData, targetData) {
				mismatchSet[paginationKey] = struct{}{}
				break // no need to compare other columns
			}
		}
	}

	for paginationKey, sourceDecompressedColumns := range source {
		targetDecompressedColumns, exists := target[paginationKey]
		if !exists {
			mismatchSet[paginationKey] = struct{}{}
			continue
		}

		for colName, sourceData := range sourceDecompressedColumns {
			targetData, exists := targetDecompressedColumns[colName]
			if !exists || !bytes.Equal(sourceData, targetData) {
				mismatchSet[paginationKey] = struct{}{}
				break
			}
		}
	}

	return mismatchSet
}

func (v *InlineVerifier) compareHashesAndData(sourceHashes, targetHashes map[uint64][]byte, sourceData, targetData map[uint64]map[string][]byte) []uint64 {
	mismatches := v.compareHashes(sourceHashes, targetHashes)
	compressedMismatch := v.compareDecompressedData(sourceData, targetData)
	for paginationKey, _ := range compressedMismatch {
		mismatches[paginationKey] = struct{}{}
	}

	mismatchList := make([]uint64, 0, len(mismatches))

	for paginationKey, _ := range mismatches {
		mismatchList = append(mismatchList, paginationKey)
	}

	return mismatchList
}

func (v *InlineVerifier) sourceBinlogEventListener(evs []DMLEvent) error {
	if v.verifyDuringCutoverStarted.Get() {
		return fmt.Errorf("cutover has started but received binlog event!")
	}

	for _, ev := range evs {
		paginationKey, err := ev.PaginationKey()
		if err != nil {
			return err
		}

		v.reverifyStore.Add(ev.TableSchema(), paginationKey)
	}

	if v.StateTracker != nil {
		v.StateTracker.UpdateLastResumableSourceBinlogPositionForInlineVerifier(evs[len(evs)-1].ResumableBinlogPosition())
	}

	return nil
}

// Verify that all DMLs against the target are coming from Ghostferry for the
// duration of the move. Once cutover has completed, we no longer need to perform
// this verification as all writes from the application are directed to the target
func (v *InlineVerifier) targetBinlogEventListener(evs []DMLEvent) error {
	// Cutover has completed, we do not need to verify target writes
	if v.cutoverCompleted.Get() {
		return nil
	}

	for _, ev := range evs {
		annotations, err := ev.Annotations()
		if err != nil {
			return err
		}

		foundAnnotation := false
		for _, annotation := range annotations {
			if strings.Contains(annotation, v.TargetDB.Marginalia) {
				foundAnnotation = true
				continue
			}
		}

		if !foundAnnotation {
			paginationKey, err := ev.PaginationKey()
			if err != nil {
				return err
			}
			return fmt.Errorf("row data with paginationKey %d on `%s`.`%s` has been corrupted", paginationKey, ev.Database(), ev.Table())
		}
	}

	if v.StateTracker != nil {
		v.StateTracker.UpdateLastResumableTargetBinlogPositionForInlineVerifier(evs[len(evs)-1].ResumableBinlogPosition())
	}

	return nil
}

func (v *InlineVerifier) readdMismatchedPaginationKeysToBeVerifiedAgain(mismatches map[string]map[string][]uint64) {
	for schemaName, _ := range mismatches {
		for tableName, paginationKeys := range mismatches[schemaName] {
			table := v.TableSchemaCache.Get(schemaName, tableName)
			for _, paginationKey := range paginationKeys {
				v.reverifyStore.Add(table, paginationKey)
			}
		}
	}
}

// Returns mismatches in the form of db -> table -> paginationKeys
func (v *InlineVerifier) verifyAllEventsInStore() (bool, map[string]map[string][]uint64, error) {
	mismatchFound := false
	mismatches := make(map[string]map[string][]uint64)
	allBatches := v.reverifyStore.Batches(v.BatchSize)

	if len(allBatches) == 0 {
		return mismatchFound, mismatches, nil
	}

	v.logger.WithField("batches", len(allBatches)).Debug("verifyAllEventsInStore")

	for _, batch := range allBatches {
		batchMismatches, err := v.verifyBinlogBatch(batch)
		if err != nil {
			return false, nil, err
		}
		v.reverifyStore.RemoveVerifiedBatch(batch)

		if len(batchMismatches) > 0 {
			mismatchFound = true

			if _, exists := mismatches[batch.SchemaName]; !exists {
				mismatches[batch.SchemaName] = make(map[string][]uint64)
			}

			if _, exists := mismatches[batch.SchemaName][batch.TableName]; !exists {
				mismatches[batch.SchemaName][batch.TableName] = make([]uint64, 0)
			}

			mismatches[batch.SchemaName][batch.TableName] = append(mismatches[batch.SchemaName][batch.TableName], batchMismatches...)
		}
	}

	return mismatchFound, mismatches, nil
}

// Returns a list of mismatched PaginationKeys.
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
				batch.PaginationKeys,
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
				batch.PaginationKeys,
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
