package ghostferry

import (
	"sort"
)

type Sortable struct {
	TableSchema      *TableSchema
	MaxPaginationKey uint64
}

type ByMaxKeyDesc []*Sortable

func (b ByMaxKeyDesc) Sort(tableSchemas map[*TableSchema]uint64) ByMaxKeyDesc {
	var sortable = make(ByMaxKeyDesc, 0)

	for t, k := range tableSchemas {
		sortable = append(sortable, &Sortable{
			TableSchema:      t,
			MaxPaginationKey: k,
		})
	}

	sort.Sort(sortable)
	return sortable
}

func (b ByMaxKeyDesc) Len() int {
	return len(b)
}

func (b ByMaxKeyDesc) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByMaxKeyDesc) Less(i, j int) bool {
	return b[i].MaxPaginationKey > b[j].MaxPaginationKey
}
