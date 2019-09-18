package copydb

import "github.com/Shopify/ghostferry/v2"

type StaticTableFilter struct {
	Dbs            []string
	DbsIsBlacklist bool

	Tables            []string
	TablesIsBlacklist bool
}

func contains(s []string, item string) bool {
	for _, v := range s {
		if item == v {
			return true
		}
	}

	return false
}

func NewStaticTableFilter(dbs, tables FilterAndRewriteConfigs) *StaticTableFilter {
	f := &StaticTableFilter{}
	// default scenario is blacklist, which would mean nothing is filtered if
	// whitelist == [] and blacklist == []
	f.DbsIsBlacklist = len(dbs.Whitelist) == 0
	f.TablesIsBlacklist = len(tables.Whitelist) == 0

	if f.DbsIsBlacklist {
		f.Dbs = dbs.Blacklist
	} else {
		f.Dbs = dbs.Whitelist
	}

	if f.TablesIsBlacklist {
		f.Tables = tables.Blacklist
	} else {
		f.Tables = tables.Whitelist
	}

	return f
}

func (s *StaticTableFilter) ApplicableDatabases(dbs []string) ([]string, error) {
	applicableDbs := make([]string, 0, len(dbs))
	for _, name := range dbs {
		var applicable bool

		b := contains(s.Dbs, name)
		if s.DbsIsBlacklist {
			applicable = !b
		} else {
			applicable = b
		}

		if applicable {
			applicableDbs = append(applicableDbs, name)
		}
	}

	return applicableDbs, nil
}

func (s *StaticTableFilter) ApplicableTables(tables []*ghostferry.TableSchema) ([]*ghostferry.TableSchema, error) {
	applicableTables := make([]*ghostferry.TableSchema, 0, len(tables))

	for _, tableSchema := range tables {
		var applicable bool

		b := contains(s.Tables, tableSchema.Name)
		if s.TablesIsBlacklist {
			applicable = !b
		} else {
			applicable = b
		}

		if applicable {
			applicableTables = append(applicableTables, tableSchema)
		}
	}

	return applicableTables, nil
}
