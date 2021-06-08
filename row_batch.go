package ghostferry

import (
	"encoding/json"
	"strings"
)

type RowBatch struct {
	values             []RowData
	paginationKeyIndex int
	table              *TableSchema
	fingerprints       map[uint64][]byte
	columns            []string
}

func NewRowBatch(table *TableSchema, values []RowData, paginationKeyIndex int) *RowBatch {
	return &RowBatch{
		values:             values,
		paginationKeyIndex: paginationKeyIndex,
		table:              table,
		columns:            ConvertTableColumnsToStrings(table.Columns),
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
