package ghostferry

import (
	"github.com/sirupsen/logrus"
	"math"
	"strings"
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
	logrus.Infof("splitting %d records in %d batches", e.Size(), len(chunks))

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
	chunkSize := int(math.Ceil(float64(len(values)) / float64(numChunks)))
	for chunkSize < len(values) {
		values, chunks = values[chunkSize:], append(chunks, values[0:chunkSize:chunkSize])
	}
	return append(chunks, values)
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
