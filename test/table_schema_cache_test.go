package test

import (
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type TableSchemaCacheTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite
	tablenames []string
}

func (this *TableSchemaCacheTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	this.tablenames = []string{"test_table_1", "test_table_2", "test_table_3"}
	for _, tablename := range this.tablenames {
		err := testhelpers.SeedInitialData(this.Ferry.SourceDB, testhelpers.TestSchemaName, tablename, 0)
		this.Require().Nil(err)
	}
}

func (this *TableSchemaCacheTestSuite) TearDownTest() {
	this.GhostferryUnitTestSuite.TearDownTest()
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesWithoutFiltering() {
	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, map[string]bool{testhelpers.TestSchemaName: true}, nil)
	this.Require().Nil(err)
	this.Require().Equal(len(this.tablenames), len(tables))
	for _, tablename := range this.tablenames {
		_, exists := tables[fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, tablename)]
		this.Require().True(exists)
	}
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesWithWhitelist() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		map[string]bool{"test_table_2": true},
	)

	this.Require().Nil(err)
	this.Require().Equal(1, len(tables))
	_, exists := tables[fmt.Sprintf("%s.test_table_2", testhelpers.TestSchemaName)]
	this.Require().True(exists)
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesWithBlacklist() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		map[string]bool{"test_table_2": false, "!default": true},
	)

	this.Require().Nil(err)
	this.Require().Equal(len(this.tablenames)-1, len(tables))
	_, exists := tables[fmt.Sprintf("%s.test_table_2", testhelpers.TestSchemaName)]
	this.Require().False(exists)

	for _, tablename := range this.tablenames {
		if tablename == "test_table_2" {
			continue
		}

		_, exists := tables[fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, tablename)]
		this.Require().True(exists)
	}
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesRejectTablesWithoutProperPK() {
	query := fmt.Sprintf("CREATE TABLE %s.%s (id varchar(20) not null, data TEXT, primary key(id))", testhelpers.TestSchemaName, "test_table_4")
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	_, err = ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		nil,
	)

	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "non-numeric primary key")
	this.Require().Contains(err.Error(), "test_table_4")
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesRejectTablesWithoutAnyPK() {
	query := fmt.Sprintf("CREATE TABLE %s.%s (id bigint(20) not null, data TEXT)", testhelpers.TestSchemaName, "test_table_4")
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	_, err = ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		nil,
	)

	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "table test_table_4 has 0 primary key columns")
}

func (this *TableSchemaCacheTestSuite) TestValuesMapTurnArrayIntoMapOfColumnNameToValues() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		nil,
	)
	this.Require().Nil(err)

	values, err := tables.ValuesMap(testhelpers.TestSchemaName, this.tablenames[0], []interface{}{1, "data"})
	this.Require().Nil(err)

	this.Require().Equal(1, values["`id`"])
	this.Require().Equal("data", values["`data`"])
}

func TestTableSchemaCache(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &TableSchemaCacheTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
