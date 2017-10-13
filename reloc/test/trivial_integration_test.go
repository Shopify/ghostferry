package test

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/Shopify/ghostferry/reloc"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
)

func addTenantID(db *sql.DB, dbname, tablename string, numberOfTenants int) {
	query := "ALTER TABLE %s.%s ADD tenant_id bigint(20)"
	query = fmt.Sprintf(query, dbname, tablename)
	_, err := db.Exec(query)
	testhelpers.PanicIfError(err)

	query = "UPDATE %s.%s SET tenant_id = id %% ?"
	query = fmt.Sprintf(query, dbname, tablename)
	_, err = db.Exec(query, numberOfTenants)
	testhelpers.PanicIfError(err)
}

func setupSingleTableDatabase(f *testhelpers.TestFerry) {
	testhelpers.SeedInitialData(f.SourceDB, "gftest", "table1", 1000)
	testhelpers.SeedInitialData(f.TargetDB, "gftest", "table1", 0)

	addTenantID(f.SourceDB, "gftest", "table1", 3)
	addTenantID(f.TargetDB, "gftest", "table1", 3)
}

func selectiveFerry(shardingValue interface{}) *testhelpers.TestFerry {
	ferry := testhelpers.NewTestFerry()
	ferry.Config.WebBasedir = "../.."

	ferry.Filter = &reloc.ShardingFilter{
		ShardingKey:   "tenant_id",
		ShardingValue: shardingValue,
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

func TestSelectiveCopyDataWithInsertLoadOnOtherTenants(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		Ferry:       selectiveFerry(int64(2)),
		SetupAction: setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},

			ExtraInsertData: func(vals map[string]interface{}) {
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
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		Ferry:       selectiveFerry(int64(2)),
		SetupAction: setupSingleTableDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},

			ExtraInsertData: func(vals map[string]interface{}) {
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
	assert.True(t, len(rows) > 333)
}
