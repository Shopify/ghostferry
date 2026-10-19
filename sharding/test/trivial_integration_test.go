package test

import (
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/Shopify/ghostferry"
	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry/sharding"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupSingleTableDatabase(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
	testhelpers.SeedInitialData(sourceDB, "gftest", "table1", 1000)
	testhelpers.SeedInitialData(targetDB, "gftest", "table1", 0)

	testhelpers.AddTenantID(sourceDB, "gftest", "table1", 3)
	testhelpers.AddTenantID(targetDB, "gftest", "table1", 3)
}

func selectiveFerry(tenantId interface{}) *testhelpers.TestFerry {
	ferry := testhelpers.NewTestFerry()

	ferry.Config.CopyFilter = &sharding.ShardedCopyFilter{
		ShardingKey:   "tenant_id",
		ShardingValue: tenantId,
	}

	ferry.Config.TableFilter = &sharding.ShardedTableFilter{
		ShardingKey: "tenant_id",
		SourceShard: "gftest",
	}

	return ferry
}

func TestSelectiveCopyDataWithoutAnyWritesToSource(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		Ferry:       selectiveFerry(int64(2)),
		SetupAction: setupSingleTableDatabase,
	}

	defer testcase.Teardown()
	testcase.CopyData()

	var count int
	row := testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id <> 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 0, count)

	rows := testcase.AssertQueriesHaveEqualResult("SELECT * FROM gftest.table1 WHERE tenant_id = 2")
	assert.Equal(t, 333, len(rows))
}

func TestSelectiveCopyIncludesMinimumKeysAndIgnoresExcludedNegativeKeys(t *testing.T) {
	ferry := testhelpers.NewTestFerry()
	ferry.Config.UpdatableConfig.DataIterationBatchSize = 1
	ferry.Config.DataIterationConcurrency = 1
	ferry.Config.VerifierType = ghostferry.VerifierTypeInline
	ferry.Config.TableFilter = &testhelpers.TestTableFilter{
		DbsFunc: testhelpers.DbApplicabilityFilter([]string{"gftest"}),
	}
	ferry.Config.CopyFilter = &sharding.ShardedCopyFilter{
		ShardingKey:   "tenant_id",
		ShardingValue: int64(0),
		PrimaryKeyTables: map[string]struct{}{
			"tenants": {},
		},
		JoinedTables: map[string][]sharding.JoinTable{
			"shared_rows": {{TableName: "links", JoinColumn: "shared_id"}},
		},
	}
	require.NoError(t, ferry.Config.ValidateConfig())
	errorHandler := &testhelpers.ErrorHandler{}
	ferry.ErrorHandler = errorHandler
	testcase := &testhelpers.IntegrationTestCase{
		T:     t,
		Ferry: ferry,
		SetupAction: func(_ *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			for _, db := range []*sql.DB{sourceDB, targetDB} {
				for _, statement := range []string{
					"CREATE DATABASE gftest",
					"CREATE TABLE gftest.normal_rows (id bigint PRIMARY KEY, tenant_id bigint NOT NULL, data TEXT, KEY tenant_idx (tenant_id, id))",
					"CREATE TABLE gftest.tenants (tenant_id bigint PRIMARY KEY, data TEXT)",
					"CREATE TABLE gftest.links (id bigint PRIMARY KEY, tenant_id bigint NOT NULL, shared_id bigint NOT NULL, KEY tenant_idx (tenant_id, id))",
					"CREATE TABLE gftest.shared_rows (id bigint PRIMARY KEY, data TEXT)",
					"CREATE TABLE gftest.excluded_rows (id bigint PRIMARY KEY, tenant_id bigint NOT NULL, KEY tenant_idx (tenant_id, id))",
				} {
					_, err := db.Exec(statement)
					require.NoError(t, err)
				}
			}
			for _, statement := range []string{
				"INSERT INTO gftest.normal_rows VALUES (-1, 2, 'excluded'), (0, 0, 'zero'), (1, 0, 'one')",
				"INSERT INTO gftest.tenants VALUES (-1, 'excluded'), (0, 'zero'), (2, 'excluded')",
				"INSERT INTO gftest.links VALUES (-1, 2, -1), (0, 0, 0), (1, 0, 1)",
				"INSERT INTO gftest.shared_rows VALUES (-1, 'excluded'), (0, 'zero'), (1, 'one')",
				"INSERT INTO gftest.excluded_rows VALUES (-2, 2), (-1, 2)",
			} {
				_, err := sourceDB.Exec(statement)
				require.NoError(t, err)
			}
		},
	}
	defer testcase.Teardown()
	testcase.CopyData()
	require.NoError(t, errorHandler.LastError)

	for _, check := range []struct {
		query string
		want  string
	}{
		{"SELECT GROUP_CONCAT(CONCAT(id, ':', data) ORDER BY id) FROM gftest.normal_rows", "0:zero,1:one"},
		{"SELECT GROUP_CONCAT(CONCAT(tenant_id, ':', data) ORDER BY tenant_id) FROM gftest.tenants", "0:zero"},
		{"SELECT GROUP_CONCAT(CONCAT(id, ':', shared_id) ORDER BY id) FROM gftest.links", "0:0,1:1"},
		{"SELECT GROUP_CONCAT(CONCAT(id, ':', data) ORDER BY id) FROM gftest.shared_rows", "0:zero,1:one"},
		{"SELECT COUNT(*) FROM gftest.excluded_rows", "0"},
	} {
		var got string
		require.NoError(t, testcase.TargetDB.QueryRow(check.query).Scan(&got))
		assert.Equal(t, check.want, got, check.query)
	}
}

func TestSelectiveCopyDataWithInsertLoadOnOtherTenants(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		Ferry:       selectiveFerry(int64(2)),
		SetupAction: setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},

			ExtraInsertData: func(tableName string, vals map[string]interface{}) {
				vals["tenant_id"] = rand.Intn(2)
			},
		},
	}

	defer testcase.Teardown()
	testcase.CopyData()

	var count int
	row := testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id <> 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 0, count)

	rows := testcase.AssertQueriesHaveEqualResult("SELECT * FROM gftest.table1 WHERE tenant_id = 2")
	assert.Equal(t, 333, len(rows))
}

func TestSelectiveCopyDataWithInsertLoadOnAllTenants(t *testing.T) {
	var firstInsert atomic.Bool

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		Ferry:       selectiveFerry(int64(2)),
		SetupAction: setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},

			ExtraInsertData: func(tableName string, vals map[string]interface{}) {
				if firstInsert.CompareAndSwap(false, true) {
					vals["tenant_id"] = 2
				} else {
					vals["tenant_id"] = rand.Intn(3)
				}
			},
		},
	}

	defer testcase.Teardown()
	testcase.CopyData()

	var count int
	row := testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id <> 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 0, count)

	rows := testcase.AssertQueriesHaveEqualResult("SELECT * FROM gftest.table1 WHERE tenant_id = 2")
	assert.True(t, len(rows) > 333)
}

type ChangeShardingKeyDataWriter struct {
	db *sql.DB
}

func (this *ChangeShardingKeyDataWriter) Run() {
	this.db.Exec("UPDATE gftest.table1 SET tenant_id = 1 WHERE tenant_id = 2 LIMIT 1")
}

func (this *ChangeShardingKeyDataWriter) Stop() {}
func (this *ChangeShardingKeyDataWriter) Wait() {}

func (this *ChangeShardingKeyDataWriter) SetDB(db *sql.DB) {
	this.db = db
}

func TestErrorsIfShardingKeyChanged(t *testing.T) {
	errorHandler := &testhelpers.ErrorHandler{}
	ferry := selectiveFerry(int64(2))
	ferry.ErrorHandler = errorHandler

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		Ferry:       ferry,
		SetupAction: setupSingleTableDatabase,
		DataWriter:  &ChangeShardingKeyDataWriter{},
	}

	defer testcase.Teardown()
	testcase.CopyData()

	assert.NotNil(t, errorHandler.LastError)
	assert.Equal(t, "sharding key changed from 2 to 1", errorHandler.LastError.Error())
}
