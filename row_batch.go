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

	vcm := e.virtualColumnsMap()
	valuesStr := "(" + strings.Repeat("?,", e.activeColumnCount(vcm)-1) + "?)"
	valuesStr = strings.Repeat(valuesStr+",", len(e.values)-1) + valuesStr

	query := "INSERT IGNORE INTO " +
		QuotedTableNameFromString(schemaName, tableName) +
		" (" + e.quotedFields(vcm) + ") VALUES " + valuesStr

	return query, e.flattenRowData(vcm), nil
}

// virtualColumnsMap returns a map of given columns (by index) -> whether the column is virtual (i.e. generated).
func (e *RowBatch) virtualColumnsMap() map[int]bool {
	res := map[int]bool{}

	for i, name := range e.columns {
		isVirtual := false
		for _, c := range e.table.Columns {
			if name == c.Name && c.IsVirtual {
				isVirtual = true
				break
			}
		}

		res[i] = isVirtual
	}

	return res
}

// activeColumnCount returns the number of active (non-virtual) columns for this RowBatch.
func (e *RowBatch) activeColumnCount(vcm map[int]bool) int {
	if vcm == nil {
		return len(e.columns)
	}

	count := 0
	for _, isVirtual := range vcm {
		if !isVirtual {
			count++
		}
	}
	return count
}

// quotedFields returns a string with comma-separated quoted field names for INSERTs.
func (e *RowBatch) quotedFields(vcm map[int]bool) string {
	cols := []string{}
	for i, name := range e.columns {
		if vcm != nil && vcm[i] {
			continue
		}
		cols = append(cols, name)
	}

	return strings.Join(QuoteFields(cols), ",")
}

// flattenRowData flattens RowData values into a single array for INSERTs.
func (e *RowBatch) flattenRowData(vcm map[int]bool) []interface{} {
	rowSize := e.activeColumnCount(vcm)
	flattened := make([]interface{}, rowSize*len(e.values))

	for rowIdx, row := range e.values {
		i := 0
		for colIdx, col := range row {
			if vcm != nil && vcm[colIdx] {
				continue
			}
			flattened[rowIdx*rowSize+i] = col
			i++
		}
	}

	return flattened
}
