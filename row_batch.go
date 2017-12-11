package ghostferry

import (
	"strings"

	"github.com/siddontang/go-mysql/schema"
)

type RowBatch struct {
	values []RowData
	TableCopy
}

func NewRowBatch(table *schema.Table, values []RowData) (*RowBatch, error) {
	return &RowBatch{
		values:    values,
		TableCopy: TableCopy{table: *table},
	}, nil
}

func (e *RowBatch) Values() []RowData {
	return e.values
}

func (e *RowBatch) Size() int {
	return len(e.values)
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
