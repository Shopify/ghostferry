package ghostferry

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/snappy"
	"github.com/sirupsen/logrus"
)

type InlineVerifier struct {
	SourceDB *sql.DB
	TargetDB *sql.DB

	sourceStmtCache *StmtCache
	targetStmtCache *StmtCache
	logger          *logrus.Entry
}

func (v *InlineVerifier) VerifyBeforeCutover() error {
	// TODO: Iterate until the reverify queue is small enough
	return nil
}

func (v *InlineVerifier) VerifyDuringCutover() (VerificationResult, error) {
	// TODO: verify everything within the reverify queue.
	return VerificationResult{}, nil
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

func (v *InlineVerifier) CheckFingerprintInline(tx *sql.Tx, targetDb, targetTable string, sourceBatch *RowBatch) ([]uint64, error) {
	table := sourceBatch.TableSchema()

	fingerprintQuery := table.FingerprintQuery(targetDb, targetTable, sourceBatch.Size())
	stmt, err := v.targetStmtCache.StmtFor(v.TargetDB, fingerprintQuery)
	if err != nil {
		return nil, err
	}

	args := make([]interface{}, len(sourceBatch.Values()))
	for i, row := range sourceBatch.Values() {
		pk, err := row.GetUint64(sourceBatch.PkIndex())
		if err != nil {
			return nil, err
		}

		args[i] = pk
	}

	rows, err := tx.Stmt(stmt).Query(args...)
	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Fetch target data
	targetFingerprints := make(map[uint64][]byte)                // pk -> fingerprint
	targetDecompressedData := make(map[uint64]map[string][]byte) // pk -> columnName -> compressedData

	for rows.Next() {
		rowData, err := ScanByteRow(rows, len(columns))
		if err != nil {
			return nil, err
		}

		pk, err := strconv.ParseUint(string(rowData[0]), 10, 64)
		if err != nil {
			return nil, err
		}

		targetFingerprints[pk] = rowData[1]
		targetDecompressedData[pk] = make(map[string][]byte)

		for i := 2; i < len(columns); i++ {
			targetDecompressedData[pk][columns[i]], err = v.DecompressData(table, columns[i], rowData[i])
			if err != nil {
				return nil, err
			}
		}
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

			sourceDecompressedData[pk][col.Name], err = v.DecompressData(table, col.Name, compressedData)
		}
	}

	mismatches := v.CompareHashes(sourceFingerprints, targetFingerprints)
	if len(mismatches) > 0 {
		return mismatches, nil
	}

	mismatches = v.CompareDecompressedData(sourceDecompressedData, targetDecompressedData)
	return mismatches, nil
}

func (v *InlineVerifier) DecompressData(table *TableSchema, column string, compressed []byte) ([]byte, error) {
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

func (v *InlineVerifier) CompareHashes(source, target map[uint64][]byte) []uint64 {
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

func (v *InlineVerifier) CompareDecompressedData(source, target map[uint64]map[string][]byte) []uint64 {
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

	mismatches := make([]uint64, 0, len(mismatchSet))
	for mismatch, _ := range mismatchSet {
		mismatches = append(mismatches, mismatch)
	}

	return mismatches
}
