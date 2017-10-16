package testhelpers

import "github.com/siddontang/go-mysql/schema"

type TestApplicability struct {
	Dbs map[string]bool
}

func NewTestApplicability(dbs map[string]bool) *TestApplicability {
	return &TestApplicability{Dbs: dbs}
}

func (t *TestApplicability) ApplicableDbs(dbs []string) []string {
	applicable := make([]string, 0, len(dbs))

	for _, db := range dbs {
		if t.Dbs[db] {
			applicable = append(applicable, db)
		}
	}

	return applicable
}

func (t *TestApplicability) ApplicableTables(tables []*schema.Table) []*schema.Table {
	return tables
}
