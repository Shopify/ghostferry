package sharding

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/require"
)

func dependencyTestConfig() *Config {
	return &Config{
		Config:   testhelpers.NewTestConfig(),
		SourceDB: "gftest_dependency_source", TargetDB: "gftest_dependency_target",
		ShardingKey: "tenant_id", ShardingValue: 1,
		CutoverDependencies: &CutoverDependenciesConfig{
			MaxRowsPerTable: 1000, MaxBytes: 1024 * 1024, TimeoutSeconds: 10,
			Tables: []CutoverDependency{
				{Table: "versions", ReferenceTable: "installations", PaginationColumn: "id",
					IdentityColumns: []string{"app_id", "app_version_id"},
					JoinColumns:     []DependencyJoin{{"app_id", "app_id"}, {"app_version_id", "deployment_id"}},
					ReferenceEquals: map[string]interface{}{"development": true}, Equals: map[string]interface{}{"release_id": 0}, RequireMatch: true},
				{Table: "modules", ReferenceTable: "installations", PaginationColumn: "id",
					IdentityColumns: []string{"app_id", "app_version_id", "module_uuid"},
					JoinColumns:     []DependencyJoin{{"app_id", "app_id"}, {"app_version_id", "deployment_id"}},
					ReferenceEquals: map[string]interface{}{"development": true}, Equals: map[string]interface{}{"release_id": 0}},
			},
		},
	}
}

func dependencyFixture(t *testing.T) *ShardingFerry {
	t.Helper()
	config := dependencyTestConfig()
	config.CutoverLock.URI = "http://unused.invalid/lock"
	ferry, err := NewFerry(config)
	require.NoError(t, err)
	ferry.Ferry.SourceDB, err = config.Source.SqlDB(nil)
	require.NoError(t, err)
	ferry.Ferry.TargetDB, err = config.Target.SqlDB(nil)
	require.NoError(t, err)
	for _, side := range []struct {
		db   *sqlwrapper.DB
		name string
	}{{ferry.Ferry.SourceDB, config.SourceDB}, {ferry.Ferry.TargetDB, config.TargetDB}} {
		db, name := side.db, side.name
		_, err := db.Exec("DROP DATABASE IF EXISTS " + name)
		require.NoError(t, err)
		_, err = db.Exec("CREATE DATABASE " + name)
		require.NoError(t, err)
		t.Cleanup(func() { db.Exec("DROP DATABASE IF EXISTS " + name); db.Close() })
		for _, ddl := range []string{
			"CREATE TABLE %s.installations (id bigint NOT NULL PRIMARY KEY, tenant_id bigint NOT NULL, app_id bigint NOT NULL, deployment_id bigint NOT NULL, development boolean NOT NULL, KEY (tenant_id, id)) ENGINE=InnoDB",
			"CREATE TABLE %s.versions (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, app_id bigint NOT NULL, app_version_id bigint NOT NULL, release_id bigint NOT NULL, payload blob, UNIQUE KEY logical_identity (app_id, app_version_id)) ENGINE=InnoDB",
			"CREATE TABLE %s.modules (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, app_id bigint NOT NULL, app_version_id bigint NOT NULL, module_uuid varchar(100) NOT NULL, release_id bigint NOT NULL, payload blob, UNIQUE KEY logical_identity (app_id, app_version_id, module_uuid)) ENGINE=InnoDB",
		} {
			_, err := db.Exec(fmt.Sprintf(ddl, name))
			require.NoError(t, err)
		}
	}
	return ferry
}

func dependencyExec(t *testing.T, db *sqlwrapper.DB, query string, args ...interface{}) {
	t.Helper()
	_, err := db.Exec(query, args...)
	require.NoError(t, err)
}

func dependencyCount(t *testing.T, db *sqlwrapper.DB, table string) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&count))
	return count
}

func TestCutoverDependenciesPreserveLogicalIdentity(t *testing.T) {
	r := dependencyFixture(t)
	source, target := r.Ferry.SourceDB, r.Ferry.TargetDB
	dependencyExec(t, source, "INSERT INTO gftest_dependency_source.installations VALUES (1,1,7,42,1),(2,1,7,42,1),(3,2,7,43,1),(4,1,7,44,0)")
	dependencyExec(t, source, "INSERT INTO gftest_dependency_source.versions VALUES (100,7,42,0,?),(101,7,43,0,'other shop'),(102,7,44,10,'released')", []byte{'a', 0, 'b'})
	dependencyExec(t, source, "INSERT INTO gftest_dependency_source.modules VALUES (200,7,42,'a',0,NULL),(201,7,42,'b',0,''),(202,7,43,'a',0,'other shop')")
	dependencyExec(t, target, "INSERT INTO gftest_dependency_target.versions VALUES (900,7,42,0,?)", []byte{'a', 0, 'b'})
	dependencyExec(t, target, "INSERT INTO gftest_dependency_target.modules VALUES (901,7,42,'a',0,NULL)")
	for i := 0; i < 2; i++ {
		require.NoError(t, r.copyCutoverDependencies())
	}
	var id int
	require.NoError(t, target.QueryRow("SELECT id FROM gftest_dependency_target.versions WHERE app_id=7 AND app_version_id=42").Scan(&id))
	require.Equal(t, 900, id)
	require.NoError(t, target.QueryRow("SELECT id FROM gftest_dependency_target.modules WHERE module_uuid='a'").Scan(&id))
	require.Equal(t, 901, id)
	require.Equal(t, 1, dependencyCount(t, target, "gftest_dependency_target.versions"))
	require.Equal(t, 2, dependencyCount(t, target, "gftest_dependency_target.modules"))
}

func TestCutoverDependenciesAbortAtomically(t *testing.T) {
	for _, name := range []string{"payload", "surrogate_collision", "missing_source", "row_budget", "byte_budget", "null_is_not_empty"} {
		t.Run(name, func(t *testing.T) {
			r := dependencyFixture(t)
			source, target := r.Ferry.SourceDB, r.Ferry.TargetDB
			dependencyExec(t, source, "INSERT INTO gftest_dependency_source.installations VALUES (1,1,7,42,1)")
			dependencyExec(t, source, "INSERT INTO gftest_dependency_source.versions VALUES (100,7,42,0,'version')")
			dependencyExec(t, source, "INSERT INTO gftest_dependency_source.modules VALUES (200,7,42,'a',0,'source'),(201,7,42,'b',0,'second')")
			want := ""
			switch name {
			case "payload":
				dependencyExec(t, target, "INSERT INTO gftest_dependency_target.modules VALUES (900,7,42,'a',0,'different')")
				want = "payload mismatch"
			case "surrogate_collision":
				dependencyExec(t, target, "INSERT INTO gftest_dependency_target.modules VALUES (200,8,99,'a',0,'unrelated')")
				want = "surrogate-key collision"
			case "missing_source":
				dependencyExec(t, source, "DELETE FROM gftest_dependency_source.versions")
				want = "required source dependency is missing"
			case "row_budget":
				r.config.CutoverDependencies.MaxRowsPerTable = 1
				want = "budget"
			case "byte_budget":
				r.config.CutoverDependencies.MaxBytes = 1
				want = "budget"
			case "null_is_not_empty":
				dependencyExec(t, source, "UPDATE gftest_dependency_source.modules SET payload=NULL WHERE id=200")
				dependencyExec(t, target, "INSERT INTO gftest_dependency_target.modules VALUES (900,7,42,'a',0,'')")
				want = "payload mismatch"
			}
			before := dependencyCount(t, target, "gftest_dependency_target.modules")
			require.ErrorContains(t, r.copyCutoverDependencies(), want)
			require.Zero(t, dependencyCount(t, target, "gftest_dependency_target.versions"))
			require.Equal(t, before, dependencyCount(t, target, "gftest_dependency_target.modules"))
		})
	}
}

func TestCutoverDependenciesKeysetPagination(t *testing.T) {
	r := dependencyFixture(t)
	dependencyExec(t, r.Ferry.SourceDB, "INSERT INTO gftest_dependency_source.installations VALUES (1,1,7,42,1)")
	dependencyExec(t, r.Ferry.SourceDB, "INSERT INTO gftest_dependency_source.versions VALUES (-5,7,42,0,'version')")
	for i := 1; i <= 205; i++ {
		dependencyExec(t, r.Ferry.SourceDB, "INSERT INTO gftest_dependency_source.modules VALUES (?,7,42,?,0,'module')", i, fmt.Sprint(i))
	}
	require.NoError(t, r.copyCutoverDependencies())
	require.Equal(t, 205, dependencyCount(t, r.Ferry.TargetDB, "gftest_dependency_target.modules"))
}

func TestCutoverDependenciesSchemaValidation(t *testing.T) {
	for _, change := range []string{
		"ALTER TABLE gftest_dependency_target.versions DROP INDEX logical_identity",
		"ALTER TABLE gftest_dependency_target.versions ADD extra_column int",
		"DROP TABLE gftest_dependency_target.versions",
		"ALTER TABLE gftest_dependency_target.versions ENGINE=MyISAM",
		"CREATE TRIGGER gftest_dependency_target.dependency_trigger BEFORE INSERT ON gftest_dependency_target.versions FOR EACH ROW SET NEW.payload='changed'",
	} {
		t.Run(change, func(t *testing.T) {
			r := dependencyFixture(t)
			dependencyExec(t, r.Ferry.TargetDB, change)
			require.Error(t, r.copyCutoverDependencies())
		})
	}
}

func TestCutoverDependenciesConfigValidation(t *testing.T) {
	cases := map[string]func(*Config){
		"no callback":  func(c *Config) { c.CutoverLock.URI = "" },
		"no budget":    func(c *Config) { c.CutoverDependencies.MaxRowsPerTable = 0 },
		"no bytes":     func(c *Config) { c.CutoverDependencies.MaxBytes = 0 },
		"no deadline":  func(c *Config) { c.CutoverDependencies.TimeoutSeconds = 0 },
		"empty tables": func(c *Config) { c.CutoverDependencies.Tables = nil },
		"duplicate": func(c *Config) {
			c.CutoverDependencies.Tables = append(c.CutoverDependencies.Tables, c.CutoverDependencies.Tables[0])
		},
		"joined": func(c *Config) {
			c.JoinedTables = map[string][]JoinTable{"versions": {{TableName: "installations", JoinColumn: "deployment_id"}}}
		},
		"primary":    func(c *Config) { c.PrimaryKeyTables = []string{"versions"} },
		"identifier": func(c *Config) { c.CutoverDependencies.Tables[0].Table = "versions`" },
		"identity":   func(c *Config) { c.CutoverDependencies.Tables[0].IdentityColumns = []string{"id"} },
		"join":       func(c *Config) { c.CutoverDependencies.Tables[0].JoinColumns = nil },
		"chain":      func(c *Config) { c.CutoverDependencies.Tables[1].ReferenceTable = "versions" },
	}
	for name, modify := range cases {
		t.Run(name, func(t *testing.T) {
			c := dependencyTestConfig()
			c.CutoverLock.URI = "http://unused.invalid/lock"
			modify(c)
			_, err := NewFerry(c)
			require.Error(t, err)
		})
	}
}

func TestCutoverDependenciesRunAfterLock(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint("conflict=", conflict), func(t *testing.T) {
			r := dependencyFixture(t)
			dependencyExec(t, r.Ferry.SourceDB, "INSERT INTO gftest_dependency_source.installations VALUES (1,1,7,41,1),(2,2,7,99,1)")
			dependencyExec(t, r.Ferry.SourceDB, "INSERT INTO gftest_dependency_source.versions VALUES (100,7,41,0,'old')")
			if conflict {
				dependencyExec(t, r.Ferry.TargetDB, "INSERT INTO gftest_dependency_target.versions VALUES (900,7,42,0,'conflict')")
			}
			locked, unlocked := false, false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/lock":
					locked = true
					// This last in-flight write completes before the callback acknowledges the lock.
					dependencyExec(t, r.Ferry.SourceDB, "INSERT INTO gftest_dependency_source.versions VALUES (101,7,42,0,'final')")
					dependencyExec(t, r.Ferry.SourceDB, "INSERT INTO gftest_dependency_source.modules VALUES (201,7,42,'a',0,'final')")
					dependencyExec(t, r.Ferry.SourceDB, "UPDATE gftest_dependency_source.installations SET deployment_id=42 WHERE id=1")
				case "/unlock":
					unlocked = true
					require.Equal(t, 1, dependencyCount(t, r.Ferry.TargetDB, "gftest_dependency_target.modules"))
				default:
					t.Errorf("unexpected callback %s", req.URL.Path)
				}
			}))
			defer server.Close()
			r.config.CutoverLock.URI = server.URL + "/lock"
			r.config.CutoverUnlock.URI = server.URL + "/unlock"
			r.config.SkipTargetVerification = false
			require.NoError(t, r.Initialize())
			t.Cleanup(func() { r.Ferry.SourceDB.Close(); r.Ferry.TargetDB.Close() })
			errors := &testhelpers.ErrorHandler{}
			r.Ferry.ErrorHandler = errors
			require.NoError(t, r.Start())
			r.Run()
			require.True(t, locked)
			if conflict {
				require.ErrorContains(t, errors.LastError, "payload mismatch")
				require.False(t, unlocked)
				r.Ferry.StopTargetVerifier()
			} else {
				require.NoError(t, errors.LastError)
				require.True(t, unlocked)
				require.Equal(t, 1, dependencyCount(t, r.Ferry.TargetDB, "gftest_dependency_target.versions"))
				var version int
				require.NoError(t, r.Ferry.TargetDB.QueryRow("SELECT deployment_id FROM gftest_dependency_target.installations WHERE id=1").Scan(&version))
				require.Equal(t, 42, version)
				require.Equal(t, 1, dependencyCount(t, r.Ferry.TargetDB, "gftest_dependency_target.installations"))
			}
		})
	}
}

func TestCutoverDependenciesDeadline(t *testing.T) {
	r := dependencyFixture(t)
	r.config.CutoverDependencies.TimeoutSeconds = 1
	dependencyExec(t, r.Ferry.SourceDB, "INSERT INTO gftest_dependency_source.installations VALUES (1,1,7,42,1)")
	dependencyExec(t, r.Ferry.SourceDB, "INSERT INTO gftest_dependency_source.versions VALUES (100,7,42,0,'version')")
	dependencyExec(t, r.Ferry.TargetDB, "INSERT INTO gftest_dependency_target.versions VALUES (900,7,42,0,'version')")
	blocker, err := r.Ferry.TargetDB.Begin()
	require.NoError(t, err)
	defer blocker.Rollback()
	_, err = blocker.Exec("UPDATE gftest_dependency_target.versions SET payload='locked' WHERE id=900")
	require.NoError(t, err)
	start := time.Now()
	require.Error(t, r.copyCutoverDependencies())
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestCutoverDependenciesExcludedFromNormalCopy(t *testing.T) {
	config := dependencyTestConfig()
	config.CutoverLock.URI = "http://unused.invalid/lock"
	r, err := NewFerry(config)
	require.NoError(t, err)
	tables, err := r.config.TableFilter.ApplicableTables([]*ghostferry.TableSchema{
		{Table: &schema.Table{Name: "versions", Columns: []schema.TableColumn{{Name: "tenant_id"}}}},
		{Table: &schema.Table{Name: "installations", Columns: []schema.TableColumn{{Name: "tenant_id"}}}},
	})
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, "installations", tables[0].Name)
}

func TestCutoverDependenciesNoReferences(t *testing.T) {
	r := dependencyFixture(t)
	dependencyExec(t, r.Ferry.SourceDB, "INSERT INTO gftest_dependency_source.versions VALUES (100,7,42,0,'orphan')")
	dependencyExec(t, r.Ferry.TargetDB, "INSERT INTO gftest_dependency_target.versions VALUES (900,8,99,0,'unrelated')")
	require.NoError(t, r.copyCutoverDependencies())
	require.Equal(t, 1, dependencyCount(t, r.Ferry.TargetDB, "gftest_dependency_target.versions"))
	var id int
	require.NoError(t, r.Ferry.TargetDB.QueryRow("SELECT id FROM gftest_dependency_target.versions").Scan(&id))
	require.Equal(t, 900, id)
}

func TestCutoverDependenciesDisabled(t *testing.T) {
	r := &ShardingFerry{config: &Config{}}
	require.NoError(t, r.copyCutoverDependencies())
}
