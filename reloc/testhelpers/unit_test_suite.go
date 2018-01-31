package testhelpers

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/reloc"
	"github.com/Shopify/ghostferry/testhelpers"

	"github.com/stretchr/testify/suite"
)

const (
	sourceDbName    = "gftest1"
	targetDbName    = "gftest2"
	testTable       = "table1"
	primaryKeyTable = "single_row_table"
	joinedTableName = "joined_table"
	joinTableName   = "join_table"
	joiningKey      = "join_id"
	shardingKey     = "tenant_id"
	shardingValue   = 2
)

type RelocUnitTestSuite struct {
	suite.Suite
	server      *httptest.Server
	metricsSink chan interface{}
	metrics     *ghostferry.Metrics

	Ferry  *reloc.RelocFerry
	Config *reloc.Config

	CutoverLock   func(http.ResponseWriter, *http.Request)
	CutoverUnlock func(http.ResponseWriter, *http.Request)
	PanicCallback func(http.ResponseWriter, *http.Request)
}

func (t *RelocUnitTestSuite) SetupSuite() {
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
		case "/panic":
			if t.PanicCallback != nil {
				t.PanicCallback(w, r)
			}
		default:
			t.Fail("Unexpected callback received")
		}
	}))
}

func (t *RelocUnitTestSuite) TearDownSuite() {
	t.server.Close()
}

func (t *RelocUnitTestSuite) SetupTest() {
	t.metricsSink = make(chan interface{}, 1024)
	reloc.SetGlobalMetrics("reloc_test", t.metricsSink)

	t.setupRelocFerry()
	t.dropTestDbs()

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
}

func (t *RelocUnitTestSuite) TearDownTest() {
	t.dropTestDbs()
}

func (t *RelocUnitTestSuite) AssertTenantCopied() {
	testhelpers.AssertTwoQueriesHaveEqualResult(
		t.T(),
		t.Ferry.Ferry,
		fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = %d", sourceDbName, testTable, shardingKey, shardingValue),
		fmt.Sprintf("SELECT * FROM %s.%s", targetDbName, testTable),
	)
}

func (t *RelocUnitTestSuite) setupRelocFerry() {
	ghostferryConfig := testhelpers.NewTestConfig()

	t.Config = &reloc.Config{
		Config: ghostferryConfig,

		ShardingKey:   shardingKey,
		ShardingValue: shardingValue,

		SourceDB: sourceDbName,
		TargetDB: targetDbName,

		JoinedTables: map[string][]reloc.JoinTable{
			joinedTableName: []reloc.JoinTable{
				{TableName: joinTableName, JoinColumn: joiningKey},
			},
		},

		PrimaryKeyTables: []string{primaryKeyTable},

		CutoverLock: reloc.HTTPCallback{
			URI:     fmt.Sprintf("%s/lock", t.server.URL),
			Payload: "test_lock",
		},

		CutoverUnlock: reloc.HTTPCallback{
			URI:     fmt.Sprintf("%s/unlock", t.server.URL),
			Payload: "test_unlock",
		},

		PanicCallback: reloc.HTTPCallback{
			URI: fmt.Sprintf("%s/panic", t.server.URL),
		},
	}

	var err error
	t.Ferry, err = reloc.NewFerry(t.Config)
	t.Require().Nil(err)

	err = t.Ferry.Initialize()
	t.Require().Nil(err)
}

func (t *RelocUnitTestSuite) dropTestDbs() {
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
