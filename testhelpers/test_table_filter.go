package testhelpers

import "github.com/siddontang/go-mysql/schema"

type TestTableFilter struct {
	DbsFunc    func([]string) []string
	TablesFunc func([]*schema.Table) []*schema.Table
}

func (t *TestTableFilter) ApplicableDatabases(dbs []string) []string {
	if t.DbsFunc != nil {
		return t.DbsFunc(dbs)
	}

	return dbs
}

func (t *TestTableFilter) ApplicableTables(tables []*schema.Table) []*schema.Table {
	if t.TablesFunc != nil {
		return t.TablesFunc(tables)
	}

	return tables
}

func DbApplicabilityFilter(applicableDbs []string) func([]string) []string {
	return func(dbs []string) []string {
		applicabilityMap := make(map[string]bool)
		for _, db := range applicableDbs {
			applicabilityMap[db] = true
		}

		applicable := make([]string, 0, len(dbs))
		for _, db := range dbs {
			if applicabilityMap[db] {
				applicable = append(applicable, db)
			}
		}

		return applicable
	}
}
