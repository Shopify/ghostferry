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
	sourceSchemaName = "gftest1"
	targetSchemaName = "gftest2"
	testTable        = "table1"
	primaryKeyTable  = "tenants_table"
	joinedTableName  = "joined_table"
	joinTableName    = "join_table"
	joiningKey       = "join_id"
	shardingKey      = "tenant_id"
	shardingValue    = 2
)

type ShardingUnitTestSuite struct {
	suite.Suite
	server      *httptest.Server
	metricsSink chan interface{}
	metrics     *ghostferry.Metrics

	SourceDB *sql.DB
	TargetDB *sql.DB
	Ferry    *sharding.ShardingFerry
	Config   *sharding.Config

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

	var err error
	ghostferryConfig := testhelpers.NewTestConfig()
	t.SourceDB, err = ghostferryConfig.Source.SqlDB(nil)
	testhelpers.PanicIfError(err)

	t.TargetDB, err = ghostferryConfig.Target.SqlDB(nil)
	testhelpers.PanicIfError(err)

	t.dropTestDbs()
	testhelpers.SetupTest()

	testhelpers.SeedInitialData(t.SourceDB, sourceSchemaName, testTable, 1000)
	testhelpers.SeedInitialData(t.TargetDB, targetSchemaName, testTable, 0)

	testhelpers.SeedInitialData(t.SourceDB, sourceSchemaName, joinedTableName, 100)
	testhelpers.SeedInitialData(t.TargetDB, targetSchemaName, joinedTableName, 0)

	testhelpers.SeedInitialData(t.SourceDB, sourceSchemaName, joinTableName, 100)
	testhelpers.SeedInitialData(t.TargetDB, targetSchemaName, joinTableName, 0)

	testhelpers.AddTenantID(t.SourceDB, sourceSchemaName, testTable, 3)
	testhelpers.AddTenantID(t.TargetDB, targetSchemaName, testTable, 3)

	testhelpers.AddTenantID(t.SourceDB, sourceSchemaName, joinTableName, 3)
	testhelpers.AddTenantID(t.TargetDB, targetSchemaName, joinTableName, 3)

	addJoinID(t.SourceDB, sourceSchemaName, joinTableName)
	addJoinID(t.TargetDB, targetSchemaName, joinTableName)

	testhelpers.SeedInitialData(t.SourceDB, sourceSchemaName, primaryKeyTable, 3)
	testhelpers.SeedInitialData(t.TargetDB, targetSchemaName, primaryKeyTable, 0)

	testhelpers.SeedInitialData(t.SourceDB, sourceSchemaName, testhelpers.TestCompressedTable1Name, 0)
	testhelpers.SeedInitialData(t.TargetDB, targetSchemaName, testhelpers.TestCompressedTable1Name, 0)

	setColumnType(t.SourceDB, sourceSchemaName, testhelpers.TestCompressedTable1Name, testhelpers.TestCompressedColumn1Name, "MEDIUMBLOB")
	setColumnType(t.TargetDB, targetSchemaName, testhelpers.TestCompressedTable1Name, testhelpers.TestCompressedColumn1Name, "MEDIUMBLOB")

	testhelpers.AddTenantID(t.SourceDB, sourceSchemaName, testhelpers.TestCompressedTable1Name, 3)
	testhelpers.AddTenantID(t.TargetDB, targetSchemaName, testhelpers.TestCompressedTable1Name, 3)

	t.setupShardingFerry()
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
		fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = %d", sourceSchemaName, testTable, shardingKey, shardingValue),
		fmt.Sprintf("SELECT * FROM %s.%s", targetSchemaName, testTable),
	)
}

func (t *ShardingUnitTestSuite) setupShardingFerry() {
	ghostferryConfig := testhelpers.NewTestConfig()

	t.Config = &sharding.Config{
		Config: ghostferryConfig,

		ShardingKey:   shardingKey,
		ShardingValue: shardingValue,

		SourceSchema: sourceSchemaName,
		TargetSchema: targetSchemaName,

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
	_, err := t.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", sourceSchemaName))
	t.Require().Nil(err)

	_, err = t.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", targetSchemaName))
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
