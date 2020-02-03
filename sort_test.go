package ghostferry

import (
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sort"
	"testing"
)

func TestSortTableSchema(t *testing.T) {
	testCases := []struct {
		desc    string
		schemas ByMaxKeyDesc
		expect  ByMaxKeyDesc
	}{
		{
			desc: "Sort Desc by MaxKey",
			schemas: []*Sortable{
				{
					TableSchema: &TableSchema{
						Table: &schema.Table{Name: "dummy2"},
					},
					MaxPaginationKey: 1,
				},
				{
					TableSchema: &TableSchema{
						Table: &schema.Table{Name: "dummy1"},
					},
					MaxPaginationKey: 1000,
				},
			},
			expect: []*Sortable{
				{
					TableSchema: &TableSchema{
						Table: &schema.Table{Name: "dummy1"},
					},
					MaxPaginationKey: 1000,
				},
				{
					TableSchema: &TableSchema{
						Table: &schema.Table{Name: "dummy2"},
					},
					MaxPaginationKey: 1,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			sort.Sort(tc.schemas)
			assert.Equal(t, true, reflect.DeepEqual(tc.schemas, tc.expect))
		})
	}
}
