package test

import (
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/copydb"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type CopydbTestSuite struct {
	suite.Suite
	copydbConfig *copydb.Config
	copydbFerry  *copydb.CopydbFerry
	ferry        *ghostferry.Ferry
}

const (
	testSchemaName    = "gftest"
	renamedSchemaName = "gftest_renamed"
	testTableName     = "test_table_1"
	renamedTableName  = "test_table_1_renamed"
)

func (t *CopydbTestSuite) SetupTest() {
	t.copydbConfig = &copydb.Config{
		Config: testhelpers.NewTestConfig(),
		Databases: copydb.FilterAndRewriteConfigs{
			Whitelist: []string{testSchemaName},
			Rewrites: map[string]string{
				testSchemaName: renamedSchemaName,
			},
		},
		Tables: copydb.FilterAndRewriteConfigs{
			Whitelist: []string{testTableName},
			Rewrites: map[string]string{
				testTableName: renamedTableName,
			},
		},
	}

	// TODO: remove this hack
	t.copydbConfig.WebBasedir = "../.."

	err := t.copydbConfig.InitializeAndValidateConfig()
	t.Require().Nil(err)

	t.copydbFerry = copydb.NewFerry(t.copydbConfig)
	err = t.copydbFerry.Initialize()
	t.Require().Nil(err)

	t.ferry = t.copydbFerry.Ferry

	testhelpers.SeedInitialData(t.ferry.SourceDB, testSchemaName, testTableName, 10)
}

func (t *CopydbTestSuite) TearDownTest() {
	_, err := t.ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", testSchemaName))
	t.Require().Nil(err)
	_, err = t.ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", testSchemaName))
	t.Require().Nil(err)
	_, err = t.ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", renamedSchemaName))
	t.Require().Nil(err)
}

func (t *CopydbTestSuite) TestCreateDatabaseAndTableWithRewrites() {
	var err error
	t.copydbFerry.Ferry.Tables, err = ghostferry.LoadTables(t.ferry.SourceDB, t.copydbFerry.Ferry.TableFilter, nil, nil, nil)
	t.Require().Nil(err)

	err = t.copydbFerry.CreateDatabasesAndTables()
	t.Require().Nil(err)

	var value string
	row := t.ferry.TargetDB.QueryRow(fmt.Sprintf("SHOW DATABASES LIKE '%s'", renamedSchemaName))
	err = row.Scan(&value)
	t.Require().Nil(err)
	t.Require().Equal(renamedSchemaName, value)

	row = t.ferry.TargetDB.QueryRow(fmt.Sprintf("SHOW TABLES IN `%s` LIKE '%s'", renamedSchemaName, renamedTableName))
	err = row.Scan(&value)
	t.Require().Nil(err)
	t.Require().Equal(renamedTableName, value)
}

func (t *CopydbTestSuite) TestCreateDatabaseAndTableWithOrdering() {
	// NOTE: Here we just ensure passing a table does not cause issues in the
	// invocation. A more thorough test is done in the table-schema tests
	t.copydbConfig.TablesToBeCreatedFirst = []string{testSchemaName + "." + testTableName}
	t.TestCreateDatabaseAndTableWithRewrites()
}

func TestCopydb(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &CopydbTestSuite{})
}
