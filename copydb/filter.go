package copydb

import (
	"github.com/siddontang/go-mysql/schema"
)

type StaticTableFilter struct {
	Dbs            map[string]bool
	DbsIsBlacklist bool

	Tables            map[string]bool
	TablesIsBlacklist bool
}

func sliceToMap(slice []string) map[string]bool {
	m := make(map[string]bool)
	for _, v := range slice {
		m[v] = true
	}

	return m
}

func NewStaticTableFilter(dbs, tables FiltersRenames) *StaticTableFilter {
	f := &StaticTableFilter{}
	// default scenario is blacklist, which would mean nothing is filtered if
	// whitelist == [] and blacklist == []
	f.DbsIsBlacklist = len(dbs.Whitelist) == 0
	f.TablesIsBlacklist = len(tables.Whitelist) == 0

	if f.DbsIsBlacklist {
		f.Dbs = sliceToMap(dbs.Blacklist)
	} else {
		f.Dbs = sliceToMap(dbs.Whitelist)
	}

	if f.TablesIsBlacklist {
		f.Tables = sliceToMap(tables.Blacklist)
	} else {
		f.Tables = sliceToMap(tables.Whitelist)
	}

	return f
}

func (s *StaticTableFilter) ApplicableDatabases(dbs []string) []string {
	applicableDbs := make([]string, 0, len(dbs))
	for _, name := range dbs {
		var applicable bool

		b, _ := s.Dbs[name]
		if s.DbsIsBlacklist {
			applicable = !b
		} else {
			applicable = b
		}

		if applicable {
			applicableDbs = append(applicableDbs, name)
		}
	}

	return applicableDbs
}

func (s *StaticTableFilter) ApplicableTables(tables []*schema.Table) []*schema.Table {
	applicableTables := make([]*schema.Table, 0, len(tables))

	for _, tableSchema := range tables {
		var applicable bool

		b, _ := s.Tables[tableSchema.Name]
		if s.TablesIsBlacklist {
			applicable = !b
		} else {
			applicable = b
		}

		if applicable {
			applicableTables = append(applicableTables, tableSchema)
		}
	}

	return applicableTables
}
