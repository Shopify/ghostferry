package ghostferry

import (
	"strings"
)

type RowBatch struct {
	values       []RowData
	pkIndex      int
	table        *TableSchema
	fingerprints map[uint64][]byte
}

func NewRowBatch(table *TableSchema, values []RowData, pkIndex int) *RowBatch {
	return &RowBatch{
		values:  values,
		pkIndex: pkIndex,
		table:   table,
	}
}

func (e *RowBatch) Values() []RowData {
	return e.values
}

func (e *RowBatch) PkIndex() int {
	return e.pkIndex
}

func (e *RowBatch) ValuesContainPk() bool {
	return e.pkIndex >= 0
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

	columns := quotedColumnNames(e.table)

	valuesStr := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	valuesStr = strings.Repeat(valuesStr+",", len(e.values)-1) + valuesStr

	query := "INSERT IGNORE INTO " +
		QuotedTableNameFromString(schemaName, tableName) +
		" (" + strings.Join(columns, ",") + ") VALUES " + valuesStr

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
