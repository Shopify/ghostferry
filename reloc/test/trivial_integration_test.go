package test

import (
	"testing"

	"github.com/Shopify/ghostferry/reloc"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
)

func setupSingleTableDatabase(f *testhelpers.TestFerry) error {
	err := testhelpers.SeedInitialData(f.SourceDB, "gftest", "table1", 1000, 3)
	if err != nil {
		return err
	}

	return testhelpers.SeedInitialData(f.TargetDB, "gftest", "table1", 0, 3)
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

	row = testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 333, count)
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
			TenantValues:        []int{0, 1},
		},
	}

	defer testcase.Teardown()
	testcase.CopyData()

	var count int
	row := testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id <> 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 0, count)

	row = testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 333, count)
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
			TenantValues:        []int{0, 1, 2},
		},
	}

	defer testcase.Teardown()
	testcase.CopyData()

	var count, expectedCount int
	row := testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id <> 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 0, count)

	row = testcase.Ferry.SourceDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&expectedCount))

	row = testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, expectedCount, count)
	assert.True(t, count > 333)
}
