package copydb

import (
	"github.com/siddontang/go-mysql/schema"
)

type StaticApplicableFilter struct {
	Dbs    map[string]bool
	Tables map[string]bool
}

func NewStaticApplicableFilter(dbs, tables map[string]bool) *StaticApplicableFilter {
	return &StaticApplicableFilter{
		Dbs:    dbs,
		Tables: tables,
	}
}

func (s *StaticApplicableFilter) ApplicableDbs(dbs []string) []string {
	return filterForApplicable(dbs, s.Dbs)
}

func (s *StaticApplicableFilter) ApplicableTables(tables []*schema.Table) []*schema.Table {
	var tableNames []string
	for _, tableSchema := range tables {
		tableNames = append(tableNames, tableSchema.Name)
	}

	applicableNames := filterForApplicable(tableNames, s.Tables)

	applicableNamesMap := make(map[string]bool)
	for _, name := range applicableNames {
		applicableNamesMap[name] = true
	}

	var applicableSchemas []*schema.Table
	for _, tableSchema := range tables {
		if applicableNamesMap[tableSchema.Name] {
			applicableSchemas = append(applicableSchemas, tableSchema)
		}
	}

	return applicableSchemas
}

func filterForApplicable(list []string, applicabilityMap map[string]bool) []string {
	if applicabilityMap == nil {
		return list
	}

	applicableByDefault := applicabilityMap["ApplicableByDefault!"]

	applicableList := make([]string, 0, len(list))
	for _, v := range list {
		applicable := applicabilityMap[v]
		applicable, specified := applicabilityMap[v]
		if specified && !applicable {
			continue
		}

		if !specified && !applicableByDefault {
			continue
		}

		applicableList = append(applicableList, v)
	}

	return applicableList
}
