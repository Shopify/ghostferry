package test

import (
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"math/rand"
	"testing"

	"github.com/Shopify/ghostferry/sharding"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
)

func setupSingleTableDatabase(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
	testhelpers.SeedInitialData(sourceDB, "gftest", "table1", 100)
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
	assert.Equal(t, 33, len(rows))
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
	assert.Equal(t, 33, len(rows))
}

func TestSelectiveCopyDataWithInsertLoadOnAllTenants(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		Ferry:       selectiveFerry(int64(2)),
		SetupAction: setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},

			ExtraInsertData: func(tableName string, vals map[string]interface{}) {
				vals["tenant_id"] = rand.Intn(3)
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
	assert.True(t, len(rows) >= 33)
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
