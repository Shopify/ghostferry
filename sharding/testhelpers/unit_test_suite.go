package testhelpers

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/sharding"
	"github.com/Shopify/ghostferry/testhelpers"

	"github.com/stretchr/testify/suite"
)

const (
	SourceDBName = "gftest1"
	TargetDBName = "gftest2"

	testTable = "table1"

	primaryKeyTable = "tenants_table"

	joinedTableName = "joined_table"
	joinTableName   = "join_table"
	joiningKey      = "join_id"

	joinedThroughTableName             = "joined_through_table"
	joinTableForJoinedThroughTableName = "join_table_for_joined_through_table"

	ShardingKey   = "tenant_id"
	ShardingValue = 2
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

	testhelpers.SeedInitialData(t.SourceDB, SourceDBName, testTable, 100)
	testhelpers.SeedInitialData(t.TargetDB, TargetDBName, testTable, 0)

	testhelpers.SeedInitialData(t.SourceDB, SourceDBName, joinedTableName, 10)
	testhelpers.SeedInitialData(t.TargetDB, TargetDBName, joinedTableName, 0)

	testhelpers.SeedInitialData(t.SourceDB, SourceDBName, joinTableName, 10)
	testhelpers.SeedInitialData(t.TargetDB, TargetDBName, joinTableName, 0)

	testhelpers.AddTenantID(t.SourceDB, SourceDBName, testTable, 3)
	testhelpers.AddTenantID(t.TargetDB, TargetDBName, testTable, 3)

	testhelpers.AddTenantID(t.SourceDB, SourceDBName, joinTableName, 3)
	testhelpers.AddTenantID(t.TargetDB, TargetDBName, joinTableName, 3)

	AddJoinID(t.SourceDB, SourceDBName, joinTableName, false)
	AddJoinID(t.TargetDB, TargetDBName, joinTableName, false)

	testhelpers.SeedInitialData(t.SourceDB, SourceDBName, primaryKeyTable, 3)
	testhelpers.SeedInitialData(t.TargetDB, TargetDBName, primaryKeyTable, 0)

	testhelpers.SeedInitialData(t.SourceDB, SourceDBName, testhelpers.TestCompressedTable1Name, 0)
	testhelpers.SeedInitialData(t.TargetDB, TargetDBName, testhelpers.TestCompressedTable1Name, 0)

	setColumnType(t.SourceDB, SourceDBName, testhelpers.TestCompressedTable1Name, testhelpers.TestCompressedColumn1Name, "MEDIUMBLOB")
	setColumnType(t.TargetDB, TargetDBName, testhelpers.TestCompressedTable1Name, testhelpers.TestCompressedColumn1Name, "MEDIUMBLOB")

	testhelpers.AddTenantID(t.SourceDB, SourceDBName, testhelpers.TestCompressedTable1Name, 3)
	testhelpers.AddTenantID(t.TargetDB, TargetDBName, testhelpers.TestCompressedTable1Name, 3)

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
		fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = %d", SourceDBName, testTable, ShardingKey, ShardingValue),
		fmt.Sprintf("SELECT * FROM %s.%s", TargetDBName, testTable),
	)
}

func (t *ShardingUnitTestSuite) CreateShardingConfig() *sharding.Config {
	ghostferryConfig := testhelpers.NewTestConfig()

	shardingConfig := &sharding.Config{
		Config: ghostferryConfig,

		ShardingKey:   ShardingKey,
		ShardingValue: ShardingValue,

		SourceDB: SourceDBName,
		TargetDB: TargetDBName,

		JoinedTables: map[string][]sharding.JoinTable{
			joinedTableName: []sharding.JoinTable{
				{TableName: joinTableName, JoinColumn: joiningKey},
			},
		},
		PrimaryKeyTables: []string{primaryKeyTable},
	}

	shardingConfig.CutoverLock = ghostferry.HTTPCallback{
		URI:     fmt.Sprintf("%s/lock", t.server.URL),
		Payload: "test_lock",
	}

	shardingConfig.CutoverUnlock = ghostferry.HTTPCallback{
		URI:     fmt.Sprintf("%s/unlock", t.server.URL),
		Payload: "test_unlock",
	}

	shardingConfig.ErrorCallback = ghostferry.HTTPCallback{
		URI: fmt.Sprintf("%s/error", t.server.URL),
	}

	return shardingConfig
}

func (t *ShardingUnitTestSuite) SetupShardingFerry(config *sharding.Config) {

	t.Config = config

	var err error
	t.Ferry, err = sharding.NewFerry(t.Config)
	t.Require().Nil(err)

	err = t.Ferry.Initialize()
	t.Require().Nil(err)

	err = t.Ferry.Start()
	t.Require().Nil(err)
}

func (t *ShardingUnitTestSuite) setupShardingFerry() {
	ghostferryConfig := testhelpers.NewTestConfig()

	t.Config = &sharding.Config{
		Config: ghostferryConfig,

		ShardingKey:   ShardingKey,
		ShardingValue: ShardingValue,

		SourceDB: SourceDBName,
		TargetDB: TargetDBName,

		JoinedTables: map[string][]sharding.JoinTable{
			joinedTableName: []sharding.JoinTable{
				{TableName: joinTableName, JoinColumn: joiningKey},
			},
		},

		JoinedThroughTables: map[string]sharding.JoinThroughTable{
			joinedThroughTableName: sharding.JoinThroughTable{
				JoinTableName: joinTableForJoinedThroughTableName,
				JoinCondition: fmt.Sprintf("`%s`.`%s` = `%s`.`%s`", joinedThroughTableName, joiningKey, joinTableForJoinedThroughTableName, joiningKey),
			},
		},

		PrimaryKeyTables: []string{primaryKeyTable},
	}

	t.Config.CutoverLock = ghostferry.HTTPCallback{
		URI:     fmt.Sprintf("%s/lock", t.server.URL),
		Payload: "test_lock",
	}

	t.Config.CutoverUnlock = ghostferry.HTTPCallback{
		URI:     fmt.Sprintf("%s/unlock", t.server.URL),
		Payload: "test_unlock",
	}

	t.Config.ErrorCallback = ghostferry.HTTPCallback{
		URI: fmt.Sprintf("%s/error", t.server.URL),
	}

	var err error
	t.Ferry, err = sharding.NewFerry(t.Config)
	t.Require().Nil(err)

	err = t.Ferry.Initialize()
	t.Require().Nil(err)
}

func (t *ShardingUnitTestSuite) dropTestDbs() {
	_, err := t.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", SourceDBName))
	t.Require().Nil(err)

	_, err = t.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", TargetDBName))
	t.Require().Nil(err)
}

func AddJoinID(db *sql.DB, dbName, tableName string, nullable bool) {
	query := fmt.Sprintf("ALTER TABLE %s.%s ADD %s bigint(20)", dbName, tableName, joiningKey)
	if !nullable {
		query += " NOT NULL"
	}

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
