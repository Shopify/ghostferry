package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
)

func qe(query, defaultSchema string) *replication.QueryEvent {
	return &replication.QueryEvent{
		Query:  []byte(query),
		Schema: []byte(defaultSchema),
	}
}

func TestIsDDLQuery(t *testing.T) {
	cases := map[string]bool{
		"ALTER TABLE foo ADD COLUMN bar INT":     true,
		"  alter table foo add column bar int":   true,
		"/* hint */ ALTER TABLE foo DROP bar":    true,
		"-- comment\nALTER TABLE foo DROP bar\n": true,
		"CREATE TABLE foo (id INT)":              true,
		"DROP TABLE foo":                         true,
		"TRUNCATE TABLE foo":                     true,
		"RENAME TABLE foo TO bar":                true,
		"BEGIN":                                  false,
		"COMMIT":                                 false,
		"SAVEPOINT sp1":                          false,
		"ROLLBACK":                               false,
		"INSERT INTO foo VALUES (1)":             false,
		"SELECT 1":                               false,
		"":                                       false,
	}
	for q, expected := range cases {
		assert.Equalf(t, expected, ghostferry.IsDDLQuery([]byte(q)), "query: %q", q)
	}
}

func TestParseDDLAlter(t *testing.T) {
	stmt := ghostferry.ParseDDLFromQueryEvent(qe("ALTER TABLE `gftest`.`mytable` ADD COLUMN x INT", "fallback"))
	if assert.NotNil(t, stmt) {
		assert.Equal(t, "ALTER", stmt.DDLType)
		assert.Equal(t, "gftest", stmt.SchemaName)
		assert.Equal(t, "mytable", stmt.TableName)
	}

	stmt = ghostferry.ParseDDLFromQueryEvent(qe("ALTER TABLE mytable ADD COLUMN x INT", "fallback"))
	if assert.NotNil(t, stmt) {
		assert.Equal(t, "fallback", stmt.SchemaName)
		assert.Equal(t, "mytable", stmt.TableName)
	}

	stmt = ghostferry.ParseDDLFromQueryEvent(qe("alter online table foo drop column bar", "db"))
	if assert.NotNil(t, stmt) {
		assert.Equal(t, "ALTER", stmt.DDLType)
		assert.Equal(t, "foo", stmt.TableName)
	}
}

func TestParseDDLCreateDropTruncate(t *testing.T) {
	stmt := ghostferry.ParseDDLFromQueryEvent(qe("CREATE TABLE IF NOT EXISTS `db`.`_foo_gho` (id INT)", "x"))
	if assert.NotNil(t, stmt) {
		assert.Equal(t, "CREATE", stmt.DDLType)
		assert.Equal(t, "db", stmt.SchemaName)
		assert.Equal(t, "_foo_gho", stmt.TableName)
	}

	stmt = ghostferry.ParseDDLFromQueryEvent(qe("DROP TABLE IF EXISTS `db`.`_foo_del`", "x"))
	if assert.NotNil(t, stmt) {
		assert.Equal(t, "DROP", stmt.DDLType)
		assert.Equal(t, "_foo_del", stmt.TableName)
	}

	stmt = ghostferry.ParseDDLFromQueryEvent(qe("TRUNCATE TABLE foo", "db"))
	if assert.NotNil(t, stmt) {
		assert.Equal(t, "TRUNCATE", stmt.DDLType)
		assert.Equal(t, "foo", stmt.TableName)
	}
}

func TestParseDDLRenameSinglePair(t *testing.T) {
	stmt := ghostferry.ParseDDLFromQueryEvent(qe("RENAME TABLE `db`.`old` TO `db`.`new`", "fallback"))
	if assert.NotNil(t, stmt) {
		assert.Equal(t, "RENAME", stmt.DDLType)
		assert.Equal(t, 1, len(stmt.RenamePairs))
		assert.Equal(t, "old", stmt.RenamePairs[0].From.TableName)
		assert.Equal(t, "new", stmt.RenamePairs[0].To.TableName)
	}
}

func TestParseDDLRenameGhostSwap(t *testing.T) {
	stmt := ghostferry.ParseDDLFromQueryEvent(qe(
		"RENAME TABLE `db`.`foo` TO `db`.`_foo_del`, `db`.`_foo_gho` TO `db`.`foo`",
		"fallback"))
	if assert.NotNil(t, stmt) {
		assert.Equal(t, "RENAME", stmt.DDLType)
		assert.Equal(t, 2, len(stmt.RenamePairs))
		assert.Equal(t, "foo", stmt.RenamePairs[0].From.TableName)
		assert.Equal(t, "_foo_del", stmt.RenamePairs[0].To.TableName)
		assert.Equal(t, "_foo_gho", stmt.RenamePairs[1].From.TableName)
		assert.Equal(t, "foo", stmt.RenamePairs[1].To.TableName)

		all := stmt.AllAffectedTables()
		assert.Equal(t, 4, len(all))
	}
}

func TestParseDDLNonDDL(t *testing.T) {
	for _, q := range []string{"BEGIN", "COMMIT", "INSERT INTO foo VALUES (1)", ""} {
		stmt := ghostferry.ParseDDLFromQueryEvent(qe(q, "db"))
		assert.Nilf(t, stmt, "expected nil for non-DDL query %q", q)
	}
}

func TestShadowTableDetection(t *testing.T) {
	cases := map[string]string{
		"_foo_gho":     "foo",
		"_foo_del":     "foo",
		"_foo_ghc":     "foo",
		"_foo_new":     "foo",
		"_foo_old":     "foo",
		"_my_table_gho": "my_table",
		"foo":          "",
		"foo_gho":      "",
		"_foo_other":   "",
	}
	for name, expectedOriginal := range cases {
		assert.Equalf(t, expectedOriginal != "", ghostferry.IsShadowTable(name), "IsShadowTable(%q)", name)
		assert.Equalf(t, expectedOriginal, ghostferry.OriginalTableFromShadow(name), "OriginalTableFromShadow(%q)", name)
	}
}
