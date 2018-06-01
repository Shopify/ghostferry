package test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
)

func setupMigrationSafetyDatabase(f *testhelpers.TestFerry) {
	testhelpers.SeedInitialData(f.SourceDB, "gftest", "table1", 1000)
	testhelpers.SeedInitialData(f.TargetDB, "gftest", "table1", 0)

	testhelpers.AddTenantID(f.SourceDB, "gftest", "table1", 3)
	testhelpers.AddTenantID(f.TargetDB, "gftest", "table1", 3)
}

func selectiveMigrationSafetyFerry(tenantId interface{}) *testhelpers.TestFerry {
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

func TestAddColumn(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		Ferry:       selectiveMigrationSafetyFerry(int64(2)),
		SetupAction: setupMigrationSafetyDatabase,
		DataWriter: &testhelpers.MixedActionDataWriter{
			ProbabilityOfInsert: 1.0,
			NumberOfWriters:     2,
			Tables:              []string{"gftest.table1"},

			ExtraInsertData: func(tableName string, vals map[string]interface{}) {
				vals["tenant_id"] = rand.Intn(3)
			},
		},
	}

	batches := 0

	testcase.Ferry.BeforeBatchCopyListener = func(batch *ghostferry.RowBatch) error {
		batches++
		if batches == 1 {
			//testcase.Ferry.TargetDB.Exec("ALTER TABLE gftest.table1 ADD new_col INT")
			go func() {
				fmt.Println("!!! START ALTER")
				_, err := testcase.Ferry.SourceDB.Exec("ALTER TABLE gftest.table1 ADD new_col INT")
				fmt.Println("!!! END ALTER: ", err)
			}()
		}
		return nil
	}

	defer testcase.Teardown()
	testcase.CopyData()

	var count int
	row := testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.table1 WHERE tenant_id <> 2")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 0, count)

	testcase.Ferry.TargetDB.Exec("ALTER TABLE gftest.table1 ADD new_col INT")
	rows := testcase.AssertQueriesHaveEqualResult("SELECT * FROM gftest.table1 WHERE tenant_id = 2")
	assert.True(t, len(rows) > 333)
}
