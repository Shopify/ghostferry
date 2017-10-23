package test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry/reloc"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
)

func addJoinID(db *sql.DB, dbName, tableName string) {
	query := fmt.Sprintf("ALTER TABLE %s.%s ADD join_id bigint(20)", dbName, tableName)
	_, err := db.Exec(query)
	testhelpers.PanicIfError(err)

	query = fmt.Sprintf("UPDATE %s.%s SET join_id = id", dbName, tableName)
	_, err = db.Exec(query)
	testhelpers.PanicIfError(err)
}

func setupMultipleJoinTableDatabase(f *testhelpers.TestFerry) {
	testhelpers.SeedInitialData(f.SourceDB, "gftest", "joined_table", 100)
	testhelpers.SeedInitialData(f.TargetDB, "gftest", "joined_table", 0)

	testhelpers.SeedInitialData(f.SourceDB, "gftest", "join_table", 100)
	testhelpers.SeedInitialData(f.TargetDB, "gftest", "join_table", 0)

	testhelpers.AddTenantID(f.SourceDB, "gftest", "join_table", 3)
	testhelpers.AddTenantID(f.TargetDB, "gftest", "join_table", 3)

	addJoinID(f.SourceDB, "gftest", "join_table")
	addJoinID(f.TargetDB, "gftest", "join_table")
}

func selectiveFerryWithJoin(shardingValue interface{}) *testhelpers.TestFerry {
	ferry := testhelpers.NewTestFerry()

	ferry.Config.CopyFilter = &reloc.ShardedRowFilter{
		ShardingKey:   "tenant_id",
		ShardingValue: shardingValue,

		JoinedTables: map[string][]reloc.JoinTable{
			"joined_table": []reloc.JoinTable{
				{Name: "join_table", Column: "join_id"},
			},
		},
	}

	return ferry
}

func TestSelectiveCopyJoinTable(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		Ferry:       selectiveFerryWithJoin(int64(2)),
		SetupAction: setupMultipleJoinTableDatabase,
	}

	defer testcase.Teardown()
	testcase.CopyData()

	var count int

	// Assert both tables were copied, with rows for only tenant 2.
	rows := testcase.AssertQueriesHaveEqualResult("SELECT * FROM gftest.joined_table jdt JOIN gftest.join_table jt on jdt.id = jt.join_id WHERE jt.tenant_id = 2")
	assert.Equal(t, 33, len(rows))

	// Assert no other rows were copied.
	row := testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.joined_table")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 33, count)

	row = testcase.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest.join_table")
	testhelpers.PanicIfError(row.Scan(&count))
	assert.Equal(t, 33, count)
}
