package testhelpers

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"
	"github.com/Shopify/ghostferry/testhelpers"

	"github.com/stretchr/testify/suite"
)

const (
	sourceDbName    = "gftest1"
	targetDbName    = "gftest2"
	testTable       = "table1"
	primaryKeyTable = "tenants_table"
	joinedTableName = "joined_table"
	joinTableName   = "join_table"
	joiningKey      = "join_id"
	shardingKey     = "tenant_id"
	shardingValue   = 2
)

type ShardingUnitTestSuite struct {
	suite.Suite
	server      *httptest.Server
	metricsSink chan interface{}
	metrics     *ghostferry.Metrics

	Ferry  *sharding.ShardingFerry
	Config *sharding.Config

	CutoverLock   func(http.ResponseWriter, *http.Request)
	CutoverUnlock func(http.ResponseWriter, *http.Request)
	ErrorCallback func(http.ResponseWriter, *http.Request)
}

func (t *ShardingUnitTestSuite) SetupSuite() {
	t.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/unlock":
			if t.CutoverUnlock != nil {
				t.CutoverUnlock(w, r)
			}
		case "/lock":
			if t.CutoverLock != nil {
				t.CutoverLock(w, r)
			}
		case "/error":
			if t.ErrorCallback != nil {
				t.ErrorCallback(w, r)
			}
		default:
			t.Fail("Unexpected callback received")
		}
	}))
}

func (t *ShardingUnitTestSuite) TearDownSuite() {
	t.server.Close()
}

func (t *ShardingUnitTestSuite) SetupTest() {
	t.metricsSink = make(chan interface{}, 1024)
	sharding.SetGlobalMetrics("sharding_test", t.metricsSink)

	t.setupShardingFerry()
	t.dropTestDbs()

	testhelpers.SetupTest()

	testhelpers.SeedInitialData(t.Ferry.Ferry.SourceDB, sourceDbName, testTable, 1000)
	testhelpers.SeedInitialData(t.Ferry.Ferry.TargetDB, targetDbName, testTable, 0)

	testhelpers.SeedInitialData(t.Ferry.Ferry.SourceDB, sourceDbName, joinedTableName, 100)
	testhelpers.SeedInitialData(t.Ferry.Ferry.TargetDB, targetDbName, joinedTableName, 0)

	testhelpers.SeedInitialData(t.Ferry.Ferry.SourceDB, sourceDbName, joinTableName, 100)
	testhelpers.SeedInitialData(t.Ferry.Ferry.TargetDB, targetDbName, joinTableName, 0)

	testhelpers.AddTenantID(t.Ferry.Ferry.SourceDB, sourceDbName, testTable, 3)
	testhelpers.AddTenantID(t.Ferry.Ferry.TargetDB, targetDbName, testTable, 3)

	testhelpers.AddTenantID(t.Ferry.Ferry.SourceDB, sourceDbName, joinTableName, 3)
	testhelpers.AddTenantID(t.Ferry.Ferry.TargetDB, targetDbName, joinTableName, 3)

	addJoinID(t.Ferry.Ferry.SourceDB, sourceDbName, joinTableName)
	addJoinID(t.Ferry.Ferry.TargetDB, targetDbName, joinTableName)

	testhelpers.SeedInitialData(t.Ferry.Ferry.SourceDB, sourceDbName, primaryKeyTable, 3)
	testhelpers.SeedInitialData(t.Ferry.Ferry.TargetDB, targetDbName, primaryKeyTable, 0)

	testhelpers.SeedInitialData(t.Ferry.Ferry.SourceDB, sourceDbName, testhelpers.TestCompressedTable1Name, 0)
	testhelpers.SeedInitialData(t.Ferry.Ferry.TargetDB, targetDbName, testhelpers.TestCompressedTable1Name, 0)

	setColumnType(t.Ferry.Ferry.SourceDB, sourceDbName, testhelpers.TestCompressedTable1Name, testhelpers.TestCompressedColumn1Name, "MEDIUMBLOB")
	setColumnType(t.Ferry.Ferry.TargetDB, targetDbName, testhelpers.TestCompressedTable1Name, testhelpers.TestCompressedColumn1Name, "MEDIUMBLOB")

	testhelpers.AddTenantID(t.Ferry.Ferry.SourceDB, sourceDbName, testhelpers.TestCompressedTable1Name, 3)
	testhelpers.AddTenantID(t.Ferry.Ferry.TargetDB, targetDbName, testhelpers.TestCompressedTable1Name, 3)
}

func (t *ShardingUnitTestSuite) TearDownTest() {
	_, err := t.Ferry.Ferry.SourceDB.Exec("SET GLOBAL read_only = OFF")
	t.Require().Nil(err)

	t.dropTestDbs()
}

func (t *ShardingUnitTestSuite) AssertTenantCopied() {
	testhelpers.AssertTwoQueriesHaveEqualResult(
		t.T(),
		t.Ferry.Ferry,
		fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = %d", sourceDbName, testTable, shardingKey, shardingValue),
		fmt.Sprintf("SELECT * FROM %s.%s", targetDbName, testTable),
	)
}

func (t *ShardingUnitTestSuite) setupShardingFerry() {
	ghostferryConfig := testhelpers.NewTestConfig()

	t.Config = &sharding.Config{
		Config: ghostferryConfig,

		ShardingKey:   shardingKey,
		ShardingValue: shardingValue,

		SourceDB: sourceDbName,
		TargetDB: targetDbName,

		JoinedTables: map[string][]sharding.JoinTable{
			joinedTableName: []sharding.JoinTable{
				{TableName: joinTableName, JoinColumn: joiningKey},
			},
		},

		PrimaryKeyTables: []string{primaryKeyTable},

		CutoverLock: ghostferry.HTTPCallback{
			URI:     fmt.Sprintf("%s/lock", t.server.URL),
			Payload: "test_lock",
		},

		CutoverUnlock: ghostferry.HTTPCallback{
			URI:     fmt.Sprintf("%s/unlock", t.server.URL),
			Payload: "test_unlock",
		},

		ErrorCallback: ghostferry.HTTPCallback{
			URI: fmt.Sprintf("%s/error", t.server.URL),
		},
	}

	var err error
	t.Ferry, err = sharding.NewFerry(t.Config)
	t.Require().Nil(err)

	err = t.Ferry.Initialize()
	t.Require().Nil(err)
}

func (t *ShardingUnitTestSuite) dropTestDbs() {
	_, err := t.Ferry.Ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", sourceDbName))
	t.Require().Nil(err)

	_, err = t.Ferry.Ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", targetDbName))
	t.Require().Nil(err)
}

func addJoinID(db *sql.DB, dbName, tableName string) {
	query := fmt.Sprintf("ALTER TABLE %s.%s ADD %s bigint(20) NOT NULL", dbName, tableName, joiningKey)
	_, err := db.Exec(query)
	testhelpers.PanicIfError(err)

	query = fmt.Sprintf("UPDATE %s.%s SET %s = id", dbName, tableName, joiningKey)
	_, err = db.Exec(query)
	testhelpers.PanicIfError(err)
}

func setColumnType(db *sql.DB, schema, table, column, columnType string) {
	_, err := db.Exec(fmt.Sprintf(
		"ALTER TABLE %s.%s MODIFY %s %s",
		schema,
		table,
		column,
		columnType,
	))
	testhelpers.PanicIfError(err)
}
