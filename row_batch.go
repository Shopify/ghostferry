package ghostferry

import (
	"encoding/json"
	"strings"
)

type RowBatch struct {
	values             []RowData
	paginationKeyIndex int
	table              *TableSchema
	fingerprints       map[string][]byte
	columns            []string
}

func NewRowBatch(table *TableSchema, values []RowData, paginationKeyIndex int) *RowBatch {
	return NewRowBatchWithColumns(table, values, ConvertTableColumnsToStrings(table.Columns), paginationKeyIndex)
}

// NewRowBatchWithColumns creates a RowBatch with an explicit ordered list of
// selected column names.  Use this when the query that produced the row data
// returns columns in a different order from the schema — for example, the
// sharding copy filter issues  SELECT * … JOIN … USING(id)  which moves 'id'
// to the front of the result set.  The selectedColumns slice must match the
// order and count of values in each RowData entry.
func NewRowBatchWithColumns(table *TableSchema, values []RowData, selectedColumns []string, paginationKeyIndex int) *RowBatch {
	return &RowBatch{
		values:             values,
		paginationKeyIndex: paginationKeyIndex,
		table:              table,
		columns:            selectedColumns,
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

func (e *RowBatch) Fingerprints() map[string][]byte {
	return e.fingerprints
}

func (e *RowBatch) AsSQLQuery(schemaName, tableName string) (string, []interface{}, error) {
	if err := verifyValuesHasTheSameLengthAsColumns(e.table, e.values...); err != nil {
		return "", nil, err
	}

	// Build the INSERT column list from e.columns — the actual query-result
	// order — skipping generated columns by name.
	//
	// We must NOT use table.NonGeneratedColumnNames() here because that
	// always returns schema order.  When the SELECT query returns columns in a
	// different order (for example, the sharding copy filter uses
	//   SELECT * FROM t JOIN (SELECT id …) AS batch USING(id)
	// which moves 'id' to the front), the column names and row values would
	// be misaligned, corrupting every row written to the target.
	insertColumns := make([]string, 0, len(e.columns))
	for _, col := range e.columns {
		if e.table.IsColumnNameGenerated(col) {
			continue
		}
		insertColumns = append(insertColumns, col)
	}

	valuesStr := "(" + strings.Repeat("?,", len(insertColumns)-1) + "?)"
	valuesStr = strings.Repeat(valuesStr+",", len(e.values)-1) + valuesStr

	query := "INSERT IGNORE INTO " +
		QuotedTableNameFromString(schemaName, tableName) +
		" (" + strings.Join(QuoteFields(insertColumns), ",") + ") VALUES " + valuesStr

	return query, e.flattenRowData(), nil
}

func (e *RowBatch) flattenRowData() []interface{} {
	flattened := make([]interface{}, 0, len(e.values)*len(e.columns))

	for _, row := range e.values {
		for colIdx, col := range e.columns {
			if e.table.IsColumnNameGenerated(col) {
				continue
			}
			flattened = append(flattened, row[colIdx])
		}
	}

	return flattened
}
