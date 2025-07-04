package test

import (
	"fmt"
	"os"
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
	t.copydbConfig.ControlServerConfig.WebBasedir = "../.."

	err := t.copydbConfig.InitializeAndValidateConfig()
	t.Require().Nil(err)

	t.copydbFerry = copydb.NewFerry(t.copydbConfig)
	err = t.copydbFerry.Initialize()
	t.Require().Nil(err)

	t.ferry = t.copydbFerry.Ferry

	// Need to do this, because we will change the character set, which can cause seed initial data to fail on subsequent runs
	_, err = t.ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", testSchemaName))
	t.Require().Nil(err)

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
	t.copydbFerry.Ferry.Tables, err = ghostferry.LoadTables(t.ferry.SourceDB, t.copydbFerry.Ferry.TableFilter, nil, nil, nil, nil)
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

func (t *CopydbTestSuite) TestCreateDatabasesAndTablesAlreadyExists() {
	var err error
	t.copydbFerry.Ferry.Tables, err = ghostferry.LoadTables(t.ferry.SourceDB, t.copydbFerry.Ferry.TableFilter, nil, nil, nil, nil)
	t.Require().Nil(err)

	testhelpers.SeedInitialData(t.ferry.TargetDB, renamedSchemaName, renamedTableName, 1)

	err = t.copydbFerry.CreateDatabasesAndTables()
	t.Require().EqualError(err, "Error 1050: Table 'test_table_1_renamed' already exists")
}

func (t *CopydbTestSuite) TestCreateDatabasesAndTablesAlreadyExistsAllowed() {
	var err error
	t.copydbFerry.Ferry.Tables, err = ghostferry.LoadTables(t.ferry.SourceDB, t.copydbFerry.Ferry.TableFilter, nil, nil, nil, nil)
	t.Require().Nil(err)

	testhelpers.SeedInitialData(t.ferry.TargetDB, renamedSchemaName, renamedTableName, 1)
	t.copydbConfig.AllowExistingTargetTable = true

	err = t.copydbFerry.CreateDatabasesAndTables()
	t.Require().Nil(err)
}

func (t *CopydbTestSuite) TestCreateDatabaseCopiesTheRightCollation() {
	// Drop the default version first because we don't need it.
	_, err := t.ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", testSchemaName))
	t.Require().Nil(err)

	_, err = t.ferry.SourceDB.Exec(fmt.Sprintf("CREATE DATABASE `%s` DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci", testSchemaName))
	t.Require().Nil(err)

	query := "CREATE TABLE %s.%s (id bigint(20) not null auto_increment, data TEXT, primary key(id)) CHARACTER SET latin1 COLLATE latin1_danish_ci"
	query = fmt.Sprintf(query, testSchemaName, testTableName)
	_, err = t.ferry.SourceDB.Exec(query)
	t.Require().Nil(err)

	t.copydbFerry.Ferry.Tables, err = ghostferry.LoadTables(t.ferry.SourceDB, t.copydbFerry.Ferry.TableFilter, nil, nil, nil, nil)
	t.Require().Nil(err)

	err = t.copydbFerry.CreateDatabasesAndTables()
	t.Require().Nil(err)

	query = "SELECT default_character_set_name, default_collation_name from information_schema.schemata where schema_name = \"%s\""
	query = fmt.Sprintf(query, renamedSchemaName)

	var characterSet, collation string
	row := t.ferry.TargetDB.QueryRow(query)
	err = row.Scan(&characterSet, &collation)
	t.Require().Nil(err)

	if os.Getenv("MYSQL_VERSION") == "8.0" || os.Getenv("MYSQL_VERSION") == "8.4" {
		t.Require().Equal(characterSet, "utf8mb3")
		t.Require().Equal(collation, "utf8mb3_general_ci")
	} else {
		t.Require().Equal(characterSet, "utf8")
		t.Require().Equal(collation, "utf8_general_ci")
	}

	query = "SELECT table_collation FROM information_schema.tables WHERE table_schema = \"%s\" AND table_name = \"%s\""
	query = fmt.Sprintf(query, renamedSchemaName, renamedTableName)
	row = t.ferry.TargetDB.QueryRow(query)
	err = row.Scan(&collation)
	t.Require().Nil(err)
	t.Require().Equal(collation, "latin1_danish_ci")
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
