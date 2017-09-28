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

func TestSelectiveCopyDataWithoutAnyWritesToSource(t *testing.T) {
	ferry := testhelpers.NewTestFerry()

	ferry.Config.WebBasedir = "../.."

	ferry.Filter = &reloc.ShardingFilter{
		ShardingKey:   "tenant_id",
		ShardingValue: 2,
	}

	testcase := &testhelpers.IntegrationTestCase{
		T:     t,
		Ferry: ferry,

		SetupAction: setupSingleTableDatabase,
	}

	defer testcase.Teardown()
	testcase.CopyData()

	var count int
	row := ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id <> 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 0, count)

	row = ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 333, count)
}
