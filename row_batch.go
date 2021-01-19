package ghostferry

import (
	"github.com/sirupsen/logrus"
	"math"
	"strings"
	"encoding/json"
)

type RowBatch struct {
	values             []RowData
	paginationKeyIndex int
	table              *TableSchema
	fingerprints       map[uint64][]byte
}

func NewRowBatch(table *TableSchema, values []RowData, paginationKeyIndex int) *RowBatch {
	return &RowBatch{
		values:             values,
		paginationKeyIndex: paginationKeyIndex,
		table:              table,
	}
}

func (e *RowBatch) Values() []RowData {
	return e.values
}

func (e *RowBatch) EstimateByteSize() uint64 {
	var total int
	for _, v := range e.values {
		size, err := json.Marshal(v)
		if err != nil {
			continue
		}
		total += len(size)
	}

	return uint64(total)
}

func (e *RowBatch) PaginationKeyIndex() int {
	return e.paginationKeyIndex
}

func (e *RowBatch) ValuesContainPaginationKey() bool {
	return e.paginationKeyIndex >= 0
}

func (e *RowBatch) Size() int {
	return len(e.values)
}

func (e *RowBatch) Split(num int) ([]*RowBatch, error) {
	chunks := e.chunkValues(e.Values(), num)

	logrus.WithFields(logrus.Fields{
		"tag":     "row_batch",
		"size":    e.Size(),
		"batches": len(chunks),
	}).Infof("splitting records in batches")

	var out []*RowBatch
	for _, chunkValues := range chunks {
		chunkFingerprints, err := e.getFingerprintsForValues(chunkValues)
		if err != nil {
			return nil, err
		}

		out = append(out, &RowBatch{
			paginationKeyIndex: e.paginationKeyIndex,
			table:              e.table,
			fingerprints:       chunkFingerprints,
			values:             chunkValues,
		})
	}
	return out, nil
}

func (e *RowBatch) TableSchema() *TableSchema {
	return e.table
}

func (e *RowBatch) Fingerprints() map[uint64][]byte {
	return e.fingerprints
}

func (e *RowBatch) AsSQLQuery(schemaName, tableName string) (string, []interface{}, error) {
	if err := verifyValuesHasTheSameLengthAsColumns(e.table, e.values...); err != nil {
		return "", nil, err
	}

	columns := quotedColumnNames(e.table)

	valuesStr := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	valuesStr = strings.Repeat(valuesStr+",", len(e.values)-1) + valuesStr

	query := "INSERT IGNORE INTO " +
		QuotedTableNameFromString(schemaName, tableName) +
		" (" + strings.Join(columns, ",") + ") VALUES " + valuesStr

	return query, e.flattenRowData(), nil
}

func (e *RowBatch) chunkValues(values []RowData, numChunks int) (chunks [][]RowData) {
	numChunkedValues := 0
	numValues := len(values)

	chunkSize := int(math.Ceil(float64(len(values)) / float64(numChunks)))
	for i := 0; i < numValues; i += chunkSize {
		chunkValues := values[i:Min(chunkSize+i, numValues)]
		chunks = append(chunks, chunkValues)

		numChunkedValues += len(chunkValues)
	}

	if numChunkedValues != numValues {
		panic("total length of chunked values are not the same length as input values")
	}

	return chunks
}

func (e *RowBatch) getFingerprintsForValues(values []RowData) (map[uint64][]byte, error) {
	out := make(map[uint64][]byte, len(values))

	for _, v := range values {
		paginationKey, err := v.GetUint64(e.paginationKeyIndex)
		if err != nil {
			return nil, err
		}
		out[paginationKey] = e.fingerprints[paginationKey]
	}

	return out, nil
}

func (e *RowBatch) flattenRowData() []interface{} {
	rowSize := len(e.values[0])
	flattened := make([]interface{}, rowSize*len(e.values))

	for rowIdx, row := range e.values {
		for colIdx, col := range row {
			flattened[rowIdx*rowSize+colIdx] = col
		}
	}

	return flattened
}
