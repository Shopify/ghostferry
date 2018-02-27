package ghostferry

import (
	"strings"

	"github.com/siddontang/go-mysql/schema"
)

type RowBatch struct {
	values  []RowData
	pkIndex int
	table   schema.Table // retain a copy in case of schema change support
}

func NewRowBatch(table *schema.Table, values []RowData, pkIndex int) *RowBatch {
	return &RowBatch{
		values:  values,
		pkIndex: pkIndex,
		table:   *table,
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

func (e *RowBatch) TableSchema() *schema.Table {
	return &e.table
}

func (e *RowBatch) AsSQLQuery(target *schema.Table) (string, []interface{}, error) {
	columns, err := loadColumnsForTable(&e.table, e.values...)
	if err != nil {
		return "", nil, err
	}

	valuesStr := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	valuesStr = strings.Repeat(valuesStr+",", len(e.values)-1) + valuesStr

	query := "INSERT IGNORE INTO " +
		QuotedTableNameFromString(target.Schema, target.Name) +
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
