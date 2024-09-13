package test

import (
	"fmt"
	"math/rand"
	"net/http"
	"testing"

	"github.com/Shopify/ghostferry/sharding"
	sth "github.com/Shopify/ghostferry/sharding/testhelpers"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

const (
	joinedThroughTable             = "joined_through_table"
	joinTableForJoinedThroughTable = "join_table_for_joined_through_table"
	joiningKey                     = "join_id"
)

type JoinedThroughTablesTestSuite struct {
	*sth.ShardingUnitTestSuite

	DataWriter testhelpers.DataWriter
}

func (t *JoinedThroughTablesTestSuite) SetupTest() {
	t.ShardingUnitTestSuite.SetupTest()
	t.setupTables()

	config := t.CreateShardingConfig()
	config.JoinedThroughTables = map[string]sharding.JoinThroughTable{
		joinedThroughTable: sharding.JoinThroughTable{
			JoinTableName: joinTableForJoinedThroughTable,
			JoinCondition: fmt.Sprintf("`%s`.`%s` = `%s`.`%s`", joinedThroughTable, joiningKey, joinTableForJoinedThroughTable, joiningKey),
		},
	}
	t.SetupShardingFerry(config)
}

func (t *JoinedThroughTablesTestSuite) TestJoinedThroughTablesWithNoConcurrentUpdates() {
	t.CutoverLock = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := t.Ferry.Ferry.SourceDB.Exec("SET GLOBAL read_only = ON")
		t.Require().Nil(err)
	})

	t.Ferry.Run()

	t.assertDataMatchBetweenSourceAndTargetForTenant()
	t.assertNoRowsForOtherTenantsCopied()
}

// TODO: test cases for new inserts, updates, deletes
// TODO: test case for new inserts, updates, deletes right beforecutover
// TODO: test case for data already existing in destination thus causing conflicts on unique index
func (t *JoinedThroughTablesTestSuite) TestJoinedThroughTablesWithConcurrentInserts() {
	t.DataWriter = &testhelpers.MixedActionDataWriter{
		ProbabilityOfInsert: 1.0,
		NumberOfWriters:     2,
		Tables: []string{
			fmt.Sprintf("%s.%s", sth.SourceDBName, joinTableForJoinedThroughTable),
			fmt.Sprintf("%s.%s", sth.SourceDBName, joinedThroughTable),
		},
		ExtraInsertData: func(tableName string, vals map[string]interface{}) {
			if tableName == fmt.Sprintf("%s.%s", sth.SourceDBName, joinTableForJoinedThroughTable) {
				fmt.Println("inserting into join_table_for_joined_through_table")

				var maxJoinID int
				row := t.Ferry.Ferry.SourceDB.QueryRow("SELECT MAX(join_id) FROM gftest1.join_table_for_joined_through_table")
				err := row.Scan(&maxJoinID)
				testhelpers.PanicIfError(err)

				vals["join_id"] = maxJoinID + 1
				vals["tenant_id"] = rand.Intn(2) + 1
			} else if tableName == fmt.Sprintf("%s.%s", sth.SourceDBName, joinedThroughTable) {
				// insert test_with_secondary_table rows with max join_id
				fmt.Println("inserting into joined_through_table")

				var randomJoinID int
				row := t.Ferry.Ferry.SourceDB.QueryRow("SELECT join_id FROM gftest1.join_table_for_joined_through_table WHERE tenant_id = ? AND join_id IS NOT NULL ORDER BY RAND() LIMIT 1", sth.ShardingValue)
				err := row.Scan(&randomJoinID)
				testhelpers.PanicIfError(err)

				vals["join_id"] = randomJoinID
			}
		},
	}
	t.DataWriter.SetDB(t.Ferry.Ferry.SourceDB)

	t.CutoverLock = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("setting global read_only")
		_, err := t.Ferry.Ferry.SourceDB.Exec("SET GLOBAL read_only = ON")
		t.Require().Nil(err)

		fmt.Println("stopping data writer")
		t.DataWriter.Stop()
		t.DataWriter.Wait()
	})

	go t.DataWriter.Run()
	t.Ferry.Run()

	t.assertDataMatchBetweenSourceAndTargetForTenant()
	t.assertNoRowsForOtherTenantsCopied()
}

func (t *JoinedThroughTablesTestSuite) setupTables() {
	/*
		=== joined_through_table ===
		| id (bigint) | data (text) | join_id (bigint) |
		|-------------|-------------|-------------------|
		| 1           | *           | 1                 |
		| 2           | *           | 2                 |
		| 3           | *           | 3                 |
		| 4           | *           | 4                 |

		=== join_table_for_joined_through_table ===
		| id (bigint) | data (text) | tenant_id (bigint) | join_id (bigint) |
		|-------------|-------------|---------------------|-------------------|
		| 1           | *           | 1                   | 1                 |
		| 2           | *           | 2                   | 2                 |
		| 3           | *           | 3                   | 3                 |
		| 4           | *           | 4                   | 4                 |
		| 5           | *           | 4                   | 1                 |
		| 6           | *           | 2                   | null              |
	*/
	testhelpers.SeedInitialData(t.SourceDB, sth.SourceDBName, joinTableForJoinedThroughTable, 0)
	testhelpers.SeedInitialData(t.TargetDB, sth.TargetDBName, joinTableForJoinedThroughTable, 0)
	testhelpers.SeedInitialData(t.SourceDB, sth.SourceDBName, joinedThroughTable, 0)
	testhelpers.SeedInitialData(t.TargetDB, sth.TargetDBName, joinedThroughTable, 0)

	testhelpers.AddTenantID(t.SourceDB, sth.SourceDBName, joinTableForJoinedThroughTable, 1)
	testhelpers.AddTenantID(t.TargetDB, sth.TargetDBName, joinTableForJoinedThroughTable, 1)

	sth.AddJoinID(t.SourceDB, sth.SourceDBName, joinTableForJoinedThroughTable, true)
	sth.AddJoinID(t.TargetDB, sth.TargetDBName, joinTableForJoinedThroughTable, true)
	sth.AddJoinID(t.SourceDB, sth.SourceDBName, joinedThroughTable, true)
	sth.AddJoinID(t.TargetDB, sth.TargetDBName, joinedThroughTable, true)

	t.insertJoinData(t.SourceDB, sth.SourceDBName, 1, 1)
	t.insertJoinData(t.SourceDB, sth.SourceDBName, 2, 2)
	t.insertJoinData(t.SourceDB, sth.SourceDBName, 3, 3)
	t.insertJoinData(t.SourceDB, sth.SourceDBName, 4, 4)
	t.insertJoinData(t.SourceDB, sth.SourceDBName, 4, 1)
	t.insertJoinData(t.SourceDB, sth.SourceDBName, 2, 0)

	t.insertJoinedThroughData(t.SourceDB, sth.SourceDBName, 1)
	t.insertJoinedThroughData(t.SourceDB, sth.SourceDBName, 2)
	t.insertJoinedThroughData(t.SourceDB, sth.SourceDBName, 3)
	t.insertJoinedThroughData(t.SourceDB, sth.SourceDBName, 4)
}

func (t *JoinedThroughTablesTestSuite) insertJoinData(db *sql.DB, dbName string, tenantID int, joinID int) {
	tx, err := db.Begin()
	testhelpers.PanicIfError(err)

	query := fmt.Sprintf("INSERT INTO %s.%s (data, tenant_id, join_id) VALUES (?, ?, ?)", dbName, joinTableForJoinedThroughTable)

	var joinIDValue interface{}
	if joinID == 0 {
		joinIDValue = nil
	} else {
		joinIDValue = joinID
	}

	_, err = tx.Exec(query, testhelpers.RandData(), tenantID, joinIDValue)
	testhelpers.PanicIfError(err)

	testhelpers.PanicIfError(tx.Commit())
}

func (t *JoinedThroughTablesTestSuite) insertJoinedThroughData(db *sql.DB, dbName string, joinID int) {
	tx, err := db.Begin()
	testhelpers.PanicIfError(err)

	query := fmt.Sprintf("INSERT INTO %s.%s (data, join_id) VALUES (?, ?)", dbName, joinedThroughTable)

	_, err = tx.Exec(query, testhelpers.RandData(), joinID)
	testhelpers.PanicIfError(err)

	testhelpers.PanicIfError(tx.Commit())
}

func (t *JoinedThroughTablesTestSuite) TearDownTest() {
	t.ShardingUnitTestSuite.TearDownTest()
}

func (t *JoinedThroughTablesTestSuite) assertDataMatchBetweenSourceAndTargetForTenant() {
	// This may not be true if there are columns that are allowed to be different (eg. id, created_at, etc)
	testhelpers.AssertTwoQueriesHaveEqualResult(
		t.T(),
		t.Ferry.Ferry,
		fmt.Sprintf("SELECT * FROM %s.%s jtt JOIN %s.%s jt on jtt.join_id = jt.join_id WHERE jt.%s = %d",
			sth.SourceDBName, joinedThroughTable, sth.SourceDBName, joinTableForJoinedThroughTable, sth.ShardingKey, sth.ShardingValue,
		),
		fmt.Sprintf("SELECT * FROM %s.%s jtt JOIN %s.%s jt on jtt.join_id = jt.join_id WHERE jt.%s = %d",
			sth.TargetDBName, joinedThroughTable, sth.TargetDBName, joinTableForJoinedThroughTable, sth.ShardingKey, sth.ShardingValue,
		),
	)
}

func (t *JoinedThroughTablesTestSuite) assertNoRowsForOtherTenantsCopied() {
	var count int

	row := t.Ferry.Ferry.TargetDB.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM %s.%s jtt JOIN %s.%s jt on jtt.join_id = jt.join_id WHERE jt.%s != %d",
			sth.TargetDBName, joinedThroughTable, sth.TargetDBName, joinTableForJoinedThroughTable, sth.ShardingKey, sth.ShardingValue,
		),
	)
	testhelpers.PanicIfError(row.Scan(&count))
	t.Require().Equal(0, count)

	var sourceJoinTableCount int
	row = t.Ferry.Ferry.SourceDB.QueryRow("SELECT count(*) FROM gftest1.join_table_for_joined_through_table WHERE tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&sourceJoinTableCount))
	fmt.Println("sourceJoinTableCount", sourceJoinTableCount)

	var sourceJoinedThroughTableCount int
	row = t.Ferry.Ferry.SourceDB.QueryRow("SELECT count(DISTINCT jtt.id) FROM gftest1.joined_through_table jtt JOIN gftest1.join_table_for_joined_through_table jt on jtt.join_id = jt.join_id WHERE jt.tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&sourceJoinedThroughTableCount))
	fmt.Println("sourceJoinedThroughTableCount", sourceJoinedThroughTableCount)

	var targetJoinTableCount int
	row = t.Ferry.Ferry.TargetDB.QueryRow("SELECT count(*) FROM gftest2.join_table_for_joined_through_table WHERE tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&targetJoinTableCount))
	fmt.Println("targetJoinTableCount", targetJoinTableCount)

	var targetJoinedThroughTableCount int
	row = t.Ferry.Ferry.TargetDB.QueryRow("SELECT count(DISTINCT jtt.id) FROM gftest2.joined_through_table jtt JOIN gftest2.join_table_for_joined_through_table jt on jtt.join_id = jt.join_id WHERE jt.tenant_id = 2")
	testhelpers.PanicIfError(row.Scan(&targetJoinedThroughTableCount))
	fmt.Println("targetJoinedThroughTableCount", targetJoinedThroughTableCount)
}

func TestJoinedThroughTablesTestSuite(t *testing.T) {
	suite.Run(t, &JoinedThroughTablesTestSuite{ShardingUnitTestSuite: &sth.ShardingUnitTestSuite{}})
}
