package ghostferry

import (
	"encoding/json"
	"strings"
)

type RowBatch struct {
	values               []RowData
	paginationKeyIndex   int // Deprecated: use paginationKeyIndexes
	paginationKeyIndexes []int
	table                *TableSchema
	fingerprints         map[string][]byte
	columns              []string
}

// Deprecated: Use NewRowBatchWithIndexes
func NewRowBatch(table *TableSchema, values []RowData, paginationKeyIndex int) *RowBatch {
	return &RowBatch{
		values:               values,
		paginationKeyIndex:   paginationKeyIndex,
		paginationKeyIndexes: []int{paginationKeyIndex},
		table:                table,
		columns:              ConvertTableColumnsToStrings(table.Columns),
	}
}

func NewRowBatchWithIndexes(table *TableSchema, values []RowData, paginationKeyIndexes []int) *RowBatch {
	var legacyIndex int = -1
	if len(paginationKeyIndexes) > 0 {
		legacyIndex = paginationKeyIndexes[0]
	}
	return &RowBatch{
		values:               values,
		paginationKeyIndex:   legacyIndex,
		paginationKeyIndexes: paginationKeyIndexes,
		table:                table,
		columns:              ConvertTableColumnsToStrings(table.Columns),
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

// Deprecated: Use PaginationKeyIndexes
func (e *RowBatch) PaginationKeyIndex() int {
	return e.paginationKeyIndex
}

func (e *RowBatch) PaginationKeyIndexes() []int {
	return e.paginationKeyIndexes
}

func (e *RowBatch) ValuesContainPaginationKey() bool {
	return len(e.paginationKeyIndexes) > 0 && e.paginationKeyIndexes[0] >= 0
}

func (e *RowBatch) Size() int {
	return len(e.values)
}

func (e *RowBatch) TableSchema() *TableSchema {
	return e.table
}

func (e *RowBatch) Fingerprints() map[string][]byte {
	return e.fingerprints
}

func (e *RowBatch) AsSQLQuery(schemaName, tableName string) (string, []interface{}, error) {
	if err := verifyValuesHasTheSameLengthAsColumns(e.table, e.values...); err != nil {
		return "", nil, err
	}

	valuesStr := "(" + strings.Repeat("?,", len(e.columns)-1) + "?)"
	valuesStr = strings.Repeat(valuesStr+",", len(e.values)-1) + valuesStr

	query := "INSERT IGNORE INTO " +
		QuotedTableNameFromString(schemaName, tableName) +
		" (" + strings.Join(QuoteFields(e.columns), ",") + ") VALUES " + valuesStr

	return query, e.flattenRowData(), nil
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
