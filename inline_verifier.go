package ghostferry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/schema"
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

func (s BinlogVerifySerializedStore) EntriesCount() uint64 {
	var v uint64 = 0
	for _, dbStore := range s {
		for _, tableStore := range dbStore {
			v += uint64(len(tableStore))
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

func (s *BinlogVerifyStore) CurrentRowCount() uint64 {
	return s.currentRowCount
}

func (s *BinlogVerifyStore) CurrentEntriesCount() uint64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.store.EntriesCount()
}

func (s *BinlogVerifyStore) Serialize() BinlogVerifySerializedStore {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.store.Copy()
}

type InlineVerifierMismatches struct {
	Pk             uint64
	SourceChecksum string
	TargetChecksum string
	Mismatch       mismatch
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

	sourceStmtCache *StmtCache
	targetStmtCache *StmtCache
	logger          *logrus.Entry

	// Used only for the ControlServer initiated VerifyDuringCutover
	backgroundVerificationResultAndStatus VerificationResultAndStatus
	backgroundVerificationErr             error
	backgroundVerificationWg              *sync.WaitGroup
}

// This is called from the control server, which is triggered by pushing Run
// Verification during cutover.
// This step is necessary to ensure the binlogs are verified in Ghostferry.
func (v *InlineVerifier) StartInBackground() error {
	if v.logger == nil {
		return errors.New("this struct must be created via Ferry.NewInlineVerifier[WithoutStateTracker]")
	}

	if v.verifyDuringCutoverStarted.Get() {
		return errors.New("verification during cutover has already been started")
	}

	v.backgroundVerificationResultAndStatus = VerificationResultAndStatus{
		StartTime: time.Now(),
		DoneTime:  time.Time{},
	}
	v.backgroundVerificationErr = nil
	v.backgroundVerificationWg = &sync.WaitGroup{}

	v.logger.Info("starting InlineVerifier.VerifyDuringCutover in the background")

	v.backgroundVerificationWg.Add(1)
	go func() {
		defer func() {
			v.backgroundVerificationResultAndStatus.DoneTime = time.Now()
			v.backgroundVerificationWg.Done()
		}()

		v.backgroundVerificationResultAndStatus.VerificationResult, v.backgroundVerificationErr = v.VerifyDuringCutover()
	}()

	return nil
}

func (v *InlineVerifier) Wait() {
	v.backgroundVerificationWg.Wait()
}

func (v *InlineVerifier) Message() string {
	return fmt.Sprintf("currentRowCount = %d, currentEntryCount = %d", v.reverifyStore.CurrentRowCount(), v.reverifyStore.CurrentEntriesCount())
}

func (v *InlineVerifier) Result() (VerificationResultAndStatus, error) {
	return v.backgroundVerificationResultAndStatus, v.backgroundVerificationErr
}

func (v *InlineVerifier) CheckFingerprintInline(tx *sql.Tx, targetSchema, targetTable string, sourceBatch *RowBatch, enforceInlineVerification bool) ([]InlineVerifierMismatches, error) {
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

	mismatches := v.compareHashesAndData(sourceFingerprints, targetFingerprints, sourceDecompressedData, targetDecompressedData)

	if !enforceInlineVerification {
		for _, mismatch := range mismatches {
			v.reverifyStore.Add(table, mismatch.Pk)
		}

		if len(mismatches) > 0 {
			v.logger.WithField("mismatches", mismatches).Info("inline verification during data copy noticed mismatched pk, which is okay")
		}
	}

	return mismatches, nil
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
		for tableName, mismatches := range mismatches[schemaName] {
			tableName = fmt.Sprintf("%s.%s", schemaName, tableName)
			incorrectTables = append(incorrectTables, tableName)

			messageBuf.WriteString(tableName)
			messageBuf.WriteString(" [paginationKeys: ")
			for _, mismatch := range mismatches {
				messageBuf.WriteString(strconv.FormatUint(mismatch.Pk, 10))
				messageBuf.WriteString(" (source: ")
				messageBuf.WriteString(mismatch.SourceChecksum)
				messageBuf.WriteString(", target: ")
				messageBuf.WriteString(mismatch.TargetChecksum)
				messageBuf.WriteString(") ")
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
	if err != nil {
		return nil, nil, err
	}

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

func (v *InlineVerifier) compareHashes(source, target map[uint64][]byte) map[uint64]InlineVerifierMismatches {
	mismatchSet := map[uint64]InlineVerifierMismatches{}

	for paginationKey, targetHash := range target {
		sourceHash, exists := source[paginationKey]
		if !bytes.Equal(sourceHash, targetHash) || !exists {
			mismatchSet[paginationKey] = InlineVerifierMismatches{
				Pk:             paginationKey,
				SourceChecksum: string(sourceHash),
				TargetChecksum: string(targetHash),
			}
		}
	}

	for paginationKey, sourceHash := range source {
		targetHash, exists := target[paginationKey]
		if !bytes.Equal(sourceHash, targetHash) || !exists {
			mismatchSet[paginationKey] = InlineVerifierMismatches{
				Pk:             paginationKey,
				SourceChecksum: string(sourceHash),
				TargetChecksum: string(targetHash),
			}
		}
	}

	return mismatchSet
}

type mismatchType string

const (
	MismatchColumnMissingOnSource mismatchType = "column missing on source"
	MismatchColumnMissingOnTarget mismatchType = "column missing on target"
	MismatchRowMissingOnSource    mismatchType = "row missing on source"
	MismatchRowMissingOnTarget    mismatchType = "row missing on target"
	MismatchContentDifference     mismatchType = "content difference"
)

type mismatch struct {
	paginationKey uint64
	column        string
	mismatchType  mismatchType
}

func compareDecompressedData(source, target map[uint64]map[string][]byte) map[uint64]mismatch {
	mismatchSet := map[uint64]mismatch{}

	for paginationKey, targetDecompressedColumns := range target { // per row/pk
		sourceDecompressedColumns, exists := source[paginationKey]
		if !exists {
			// row missing on source
			mismatchSet[paginationKey] = mismatch{
				paginationKey: paginationKey,
				mismatchType:  MismatchRowMissingOnSource,
			}
			continue
		}

		for colName, targetData := range targetDecompressedColumns {
			sourceData, exists := sourceDecompressedColumns[colName]
			if !exists {
				mismatchSet[paginationKey] = mismatch{
					paginationKey: paginationKey,
					column:        colName,
					mismatchType:  MismatchColumnMissingOnSource,
				}
				break // no need to compare other columns
			} else if !bytes.Equal(sourceData, targetData) {
				mismatchSet[paginationKey] = mismatch{
					paginationKey: paginationKey,
					column:        colName,
					mismatchType:  MismatchContentDifference,
				}
				break // no need to compare other columns
			}
		}
	}

	for paginationKey, sourceDecompressedColumns := range source {
		targetDecompressedColumns, exists := target[paginationKey]
		if !exists {
			// row missing on target
			mismatchSet[paginationKey] = mismatch{
				paginationKey: paginationKey,
				mismatchType:  MismatchRowMissingOnTarget,
			}
			continue
		}

		for colName := range sourceDecompressedColumns {
			_, exists := targetDecompressedColumns[colName]
			if !exists {
				mismatchSet[paginationKey] = mismatch{
					paginationKey: paginationKey,
					column:        colName,
					mismatchType:  MismatchColumnMissingOnTarget,
				}
			}
		}
	}

	return mismatchSet
}

func (v *InlineVerifier) compareHashesAndData(sourceHashes, targetHashes map[uint64][]byte, sourceData, targetData map[uint64]map[string][]byte) []InlineVerifierMismatches {
	mismatches := v.compareHashes(sourceHashes, targetHashes)
	compressedMismatch := compareDecompressedData(sourceData, targetData)
	for paginationKey, mismatch := range compressedMismatch {
		mismatches[paginationKey] = InlineVerifierMismatches{
			Pk:             paginationKey,
			SourceChecksum: "compressed-data-mismatch", // TODO: compute the hash of the compressed data and put it here
			TargetChecksum: "compressed-data-mismatch",
			Mismatch:       mismatch,
		}
	}

	mismatchList := make([]InlineVerifierMismatches, 0, len(mismatches))

	for _, mismatch := range mismatches {
		mismatchList = append(mismatchList, mismatch)
	}

	return mismatchList
}

func (v *InlineVerifier) binlogEventListener(evs []DMLEvent) error {
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

func (v *InlineVerifier) readdMismatchedPaginationKeysToBeVerifiedAgain(mismatches map[string]map[string][]InlineVerifierMismatches) {
	for schemaName, _ := range mismatches {
		for tableName, mismatches := range mismatches[schemaName] {
			table := v.TableSchemaCache.Get(schemaName, tableName)
			for _, mismatch := range mismatches {
				v.reverifyStore.Add(table, mismatch.Pk)
			}
		}
	}
}

// Returns mismatches in the form of db -> table -> paginationKeys
func (v *InlineVerifier) verifyAllEventsInStore() (bool, map[string]map[string][]InlineVerifierMismatches, error) {
	mismatchFound := false
	mismatches := make(map[string]map[string][]InlineVerifierMismatches)
	allBatches := v.reverifyStore.Batches(v.BatchSize)

	if len(allBatches) == 0 {
		return mismatchFound, mismatches, nil
	}

	v.logger.WithField("batches", len(allBatches)).Debug("verifyAllEventsInStore")

	for _, batch := range allBatches {
		batchMismatches, err := v.verifyBinlogBatch(batch, false)
		if err != nil {
			return false, nil, err
		}
		v.reverifyStore.RemoveVerifiedBatch(batch)

		if len(batchMismatches) > 0 {
			mismatchFound = true

			if _, exists := mismatches[batch.SchemaName]; !exists {
				mismatches[batch.SchemaName] = make(map[string][]InlineVerifierMismatches)
			}

			if _, exists := mismatches[batch.SchemaName][batch.TableName]; !exists {
				mismatches[batch.SchemaName][batch.TableName] = make([]InlineVerifierMismatches, 0)
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
func (v *InlineVerifier) verifyBinlogBatch(batch BinlogVerifyBatch, skipJsonColumnCheck bool) ([]InlineVerifierMismatches, error) {
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
		return []InlineVerifierMismatches{}, fmt.Errorf("programming error? %s.%s is not found in TableSchemaCache but is being reverified", batch.SchemaName, batch.TableName)
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

	mismatches := v.compareHashesAndData(sourceFingerprints, targetFingerprints, sourceDecompressedData, targetDecompressedData)

	if len(mismatches) > 0 && !skipJsonColumnCheck {
		return v.compareJsonColumnValues(batch, mismatches, sourceTableSchema, targetSchema, targetTable)
	}

	return mismatches, nil
}

func (v *InlineVerifier) compareJsonColumnValues(batch BinlogVerifyBatch, mismatches []InlineVerifierMismatches, sourceTableSchema *TableSchema, targetSchema string, targetTable string) ([]InlineVerifierMismatches, error) {
	jsonColumnNames := []string{}
	for _, column := range sourceTableSchema.Columns {
		if column.Type == schema.TYPE_JSON {
			jsonColumnNames = append(jsonColumnNames, column.Name)
		}
	}

	if len(jsonColumnNames) == 0 {
		return mismatches, nil
	}

	addJsonColumnNamesToIgnoredColumnsForVerification(sourceTableSchema, jsonColumnNames)

	mismatches, err := v.verifyBinlogBatch(batch, true)
	if err != nil {
		return nil, err
	}

	if len(mismatches) > 0 {
		return mismatches, nil
	}

	args := make([]interface{}, len(batch.PaginationKeys))
	for i, paginationKey := range batch.PaginationKeys {
		args[i] = paginationKey
	}

	sourceQuery := jsonColumnValueQuery(sourceTableSchema, batch.SchemaName, batch.TableName, jsonColumnNames, len(batch.PaginationKeys))
	targetQuery := jsonColumnValueQuery(sourceTableSchema, targetSchema, targetTable, jsonColumnNames, len(batch.PaginationKeys))

	sourceStatement, _ := v.sourceStmtCache.StmtFor(v.SourceDB, sourceQuery)
	targetStatement, _ := v.targetStmtCache.StmtFor(v.TargetDB, targetQuery)

	sourceRows, _ := sourceStatement.Query(args...)
	targetRows, _ := targetStatement.Query(args...)

	defer sourceRows.Close()
	defer targetRows.Close()

	mismatchedJsonColumns := []string{}
	paginationKeysWithMismatchedJson := []uint64{}

	for {
		hasSourceRows := sourceRows.Next()
		hasTargetRows := targetRows.Next()

		if !hasSourceRows && !hasTargetRows {
			break
		}

		if (hasSourceRows && !hasTargetRows) || (!hasSourceRows && hasTargetRows) {
			return nil, fmt.Errorf("Number of source and target rows are different")
		}

		sourceRowData, err := ScanByteRow(sourceRows, len(jsonColumnNames)+1)
		if err != nil {
			return nil, err
		}

		targetRowData, err := ScanByteRow(targetRows, len(jsonColumnNames)+1)
		if err != nil {
			return nil, err
		}

		var sourceJsonColumnValue map[string]interface{}
		var sourcePaginationKey uint64

		var targetJsonColumnValue map[string]interface{}
		var targetPaginationKey uint64

		sourcePaginationKey, _ = strconv.ParseUint(string(sourceRowData[0]), 10, 64)
		targetPaginationKey, _ = strconv.ParseUint(string(targetRowData[0]), 10, 64)

		for j, jsonColumn := range jsonColumnNames {
			err := json.Unmarshal([]byte(sourceRowData[j+1]), &sourceJsonColumnValue)
			if err != nil {
				return nil, fmt.Errorf("unmarshalling target rowdata: %w", err)
			}

			err = json.Unmarshal([]byte(targetRowData[j+1]), &targetJsonColumnValue)
			if err != nil {
				return nil, fmt.Errorf("unmarshalling target rowdata: %w", err)
			}

			if sourcePaginationKey == targetPaginationKey && reflect.DeepEqual(sourceJsonColumnValue, targetJsonColumnValue) {
				continue
			}

			if !uint64SliceContains(paginationKeysWithMismatchedJson, sourcePaginationKey) {
				paginationKeysWithMismatchedJson = append(paginationKeysWithMismatchedJson, sourcePaginationKey)
			}

			if !stringSliceContains(mismatchedJsonColumns, jsonColumn) {
				mismatchedJsonColumns = append(mismatchedJsonColumns, jsonColumn)
			}
		}
	}

	if len(mismatchedJsonColumns) == 0 {
		return mismatches, nil
	}

	removeJsonColumnsFromIgnoredColumnsForVerification(sourceTableSchema, mismatchedJsonColumns)

	mismatched, err := v.verifyBinlogBatch(batch, true)

	if err != nil {
		return nil, err
	}

	filteredMismatches := []InlineVerifierMismatches{}

	// filtering out the mismatches that have successful json value comparison
	for _, mismatch := range mismatched {
		for _, mismatchedJsonPK := range paginationKeysWithMismatchedJson {
			if mismatch.Pk == mismatchedJsonPK {
				filteredMismatches = append(filteredMismatches, mismatch)
			}
		}
	}

	return filteredMismatches, nil
}

func jsonColumnValueQuery(sourceTableSchema *TableSchema, schemaName string, tableName string, jsonColumnNames []string, paginationKeysCount int) string {
	paginationColumn := QuoteField(sourceTableSchema.GetPaginationColumn().Name)

	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s IN (%s) ORDER BY %s",
		fmt.Sprintf("%s, %s", paginationColumn, strings.Join(jsonColumnNames, ",")),
		QuotedTableNameFromString(schemaName, tableName),
		paginationColumn,
		strings.Repeat("?,", paginationKeysCount-1)+"?",
		paginationColumn,
	)
}

func addJsonColumnNamesToIgnoredColumnsForVerification(sourceTableSchema *TableSchema, jsonColumnNames []string) {
	for _, jsonColumn := range jsonColumnNames {
		if sourceTableSchema.IgnoredColumnsForVerification == nil {
			sourceTableSchema.IgnoredColumnsForVerification = make(map[string]struct{})
		}

		sourceTableSchema.IgnoredColumnsForVerification[jsonColumn] = struct{}{}
	}

	sourceTableSchema.rowMd5Query = ""
	sourceTableSchema.RowMd5Query()
}

func removeJsonColumnsFromIgnoredColumnsForVerification(sourceTableSchema *TableSchema, jsonColumnNames []string) {
	for _, jsonColumn := range jsonColumnNames {
		delete(sourceTableSchema.IgnoredColumnsForVerification, jsonColumn)
	}

	sourceTableSchema.rowMd5Query = ""
	sourceTableSchema.RowMd5Query()
}

func uint64SliceContains(s []uint64, item uint64) bool {
	for _, v := range s {
		if item == v {
			return true
		}
	}

	return false
}

func stringSliceContains(s []string, item string) bool {
	for _, v := range s {
		if item == v {
			return true
		}
	}

	return false
}
