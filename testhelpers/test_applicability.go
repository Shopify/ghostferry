package testhelpers

import "github.com/siddontang/go-mysql/schema"

type TestApplicability struct {
	DbsFunc    func([]string) []string
	TablesFunc func([]*schema.Table) []*schema.Table
}

func (t *TestApplicability) ApplicableDatabases(dbs []string) []string {
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
