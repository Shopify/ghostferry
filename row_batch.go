package ghostferry

import (
	"encoding/json"
	"fmt"
	"strings"
)

type RowBatch struct {
	values             []RowData
	paginationKeyIndex int
	table              *TableSchema
	fingerprints       map[string][]byte
	columns            []string
	// Positions in columns that aren't generated columns.  Computed once at
	// construction; avoids re-checking each column name against the schema
	// inside the per-cell loop in flattenRowData (which would otherwise be
	// O(rows × cols²) string comparisons).
	nonGeneratedColumnIdxs []int
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
	nonGeneratedColumnIdxs := make([]int, 0, len(selectedColumns))
	for i, col := range selectedColumns {
		if table.IsColumnNameGenerated(col) {
			continue
		}
		nonGeneratedColumnIdxs = append(nonGeneratedColumnIdxs, i)
	}

	return &RowBatch{
		values:                 values,
		paginationKeyIndex:     paginationKeyIndex,
		table:                  table,
		columns:                selectedColumns,
		nonGeneratedColumnIdxs: nonGeneratedColumnIdxs,
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
	for _, row := range e.values {
		if len(e.columns) != len(row) {
			return "", nil, fmt.Errorf(
				"table %s.%s has %d selected columns but row has %d values",
				e.table.Schema,
				e.table.Name,
				len(e.columns),
				len(row),
			)
		}
	}

	// The INSERT column list must follow e.columns — the order the SELECT
	// returned — not schema order.  The two differ under the sharding copy
	// filter, whose JOIN ... USING moves the join column to the front; naming
	// columns in schema order against result-ordered values silently writes
	// every value into the wrong column (the gh-285 corruption pattern).
	insertColumns := make([]string, 0, len(e.nonGeneratedColumnIdxs))
	for _, i := range e.nonGeneratedColumnIdxs {
		insertColumns = append(insertColumns, e.columns[i])
	}

	// LoadTables refuses a table with no writable columns, but a CopyFilter
	// narrowing ColumnsToSelect, or an embedder populating Ferry.Tables
	// directly, can still arrive here with nothing to write.  Without this
	// check, strings.Repeat below panics on a negative count.
	if len(insertColumns) == 0 {
		return "", nil, fmt.Errorf(
			"table %s.%s has no columns to write: every selected column (%v) is a generated column",
			e.table.Schema,
			e.table.Name,
			e.columns,
		)
	}

	valuesStr := "(" + strings.Repeat("?,", len(insertColumns)-1) + "?)"
	valuesStr = strings.Repeat(valuesStr+",", len(e.values)-1) + valuesStr

	query := "INSERT IGNORE INTO " +
		QuotedTableNameFromString(schemaName, tableName) +
		" (" + strings.Join(QuoteFields(insertColumns), ",") + ") VALUES " + valuesStr

	return query, e.flattenRowData(), nil
}

func (e *RowBatch) flattenRowData() []interface{} {
	flattened := make([]interface{}, 0, len(e.values)*len(e.nonGeneratedColumnIdxs))
	for _, row := range e.values {
		for _, i := range e.nonGeneratedColumnIdxs {
			flattened = append(flattened, row[i])
		}
	}
	return flattened
}
