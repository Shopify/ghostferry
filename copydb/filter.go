package copydb

import (
	"github.com/siddontang/go-mysql/schema"
)

type StaticTableFilter struct {
	Dbs    map[string]bool
	Tables map[string]bool
}

func NewStaticTableFilter(dbs, tables map[string]bool) *StaticTableFilter {
	return &StaticTableFilter{
		Dbs:    dbs,
		Tables: tables,
	}
}

func (s *StaticTableFilter) ApplicableDatabases(dbs []string) []string {
	if s.Dbs == nil {
		return dbs
	}

	applicables := applicableIdxs(dbs, s.Dbs)

	applicableDbs := make([]string, len(applicables))
	for i, j := range applicables {
		applicableDbs[i] = dbs[j]
	}

	return applicableDbs
}

func (s *StaticTableFilter) ApplicableTables(tables []*schema.Table) []*schema.Table {
	if s.Tables == nil {
		return tables
	}

	tableNames := make([]string, len(tables))
	for i, tableSchema := range tables {
		tableNames[i] = tableSchema.Name
	}

	applicables := applicableIdxs(tableNames, s.Tables)

	applicableSchemas := make([]*schema.Table, len(applicables))
	for i, j := range applicables {
		applicableSchemas[i] = tables[j]
	}

	return applicableSchemas
}

func applicableIdxs(list []string, applicabilityMap map[string]bool) []int {
	applicableByDefault := applicabilityMap["ApplicableByDefault!"]

	idxs := make([]int, 0, len(list))
	for idx, element := range list {
		applicable, specified := applicabilityMap[element]
		if specified && !applicable {
			continue
		}

		if !specified && !applicableByDefault {
			continue
		}

		idxs = append(idxs, idx)
	}

	return idxs
}
