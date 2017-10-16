package testhelpers

import "github.com/siddontang/go-mysql/schema"

type TestApplicability struct {
	DbsFunc    func([]string) []string
	TablesFunc func([]*schema.Table) []*schema.Table
}

func (t *TestApplicability) ApplicableDbs(dbs []string) []string {
	if t.DbsFunc != nil {
		return t.DbsFunc(dbs)
	}

	return dbs
}

func (t *TestApplicability) ApplicableTables(tables []*schema.Table) []*schema.Table {
	if t.TablesFunc != nil {
		return t.TablesFunc(tables)
	}

	return tables
}

func DbApplicability(applicableDbs []string) func([]string) []string {
	return func(dbs []string) []string {
		return filterForApplicable(dbs, applicableDbs)
	}
}

func TableMapApplicability(applicableTables []string) func([]*schema.Table) []*schema.Table {
	return func(tables []*schema.Table) []*schema.Table {
		var tableNames []string
		for _, tableSchema := range tables {
			tableNames = append(tableNames, tableSchema.Name)
		}

		applicableNames := filterForApplicable(tableNames, applicableTables)

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
}

func filterForApplicable(list []string, applicableList []string) []string {
	applicabilityMap := make(map[string]bool)
	for _, element := range applicableList {
		applicabilityMap[element] = true
	}

	applicable := make([]string, 0, len(list))

	for _, element := range list {
		if applicabilityMap[element] {
			applicable = append(applicable, element)
		}
	}

	return applicable
}
