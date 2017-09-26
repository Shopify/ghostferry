package test

import (
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"

	sqlSchema "github.com/siddontang/go-mysql/schema"
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
		schema := tables[fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, tablename)]

		this.Require().Equal(schema.Schema, testhelpers.TestSchemaName)
		this.Require().Equal(schema.Name, tablename)
		this.Require().Equal(1, len(schema.PKColumns))
		this.Require().Equal(0, schema.PKColumns[0])

		expectedColumnNames := []string{"id", "data"}
		expectedColumnTypes := []int{sqlSchema.TYPE_NUMBER, sqlSchema.TYPE_STRING}
		for idx, column := range schema.Columns {
			this.Require().Equal(expectedColumnNames[idx], column.Name)
			this.Require().Equal(expectedColumnTypes[idx], column.Type)
		}
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

	_, exists = tables[fmt.Sprintf("%s.test_table_3", testhelpers.TestSchemaName)]
	this.Require().False(exists)
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesWithBlacklist() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		map[string]bool{"test_table_2": false, "applicableByDefault": true},
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

func (this *TableSchemaCacheTestSuite) TestLoadTablesRejectTablesWithoutNumericPK() {
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

func (this *TableSchemaCacheTestSuite) TestValuesMapErrMismatchedLength() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		nil,
	)
	this.Require().Nil(err)

	_, err = tables.ValuesMap(testhelpers.TestSchemaName, this.tablenames[0], []interface{}{1, "data", "dummy"})
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table_1 has 2 columns but row has 3 columns")
}

func (this *TableSchemaCacheTestSuite) TestValuesMapNonExistentTable() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		nil,
	)
	this.Require().Nil(err)

	_, err = tables.ValuesMap(testhelpers.TestSchemaName, "table_doesnt_exist", []interface{}{1, "data", "dummy"})
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "table_doesnt_exist does not exist")
}

func (this *TableSchemaCacheTestSuite) TestTableColumns() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		map[string]bool{this.tablenames[1]: false, "applicableByDefault": true},
	)
	this.Require().Nil(err)

	columns, err := tables.TableColumns(testhelpers.TestSchemaName, this.tablenames[0])
	this.Require().Nil(err)

	expectedColumnNames := []string{"id", "data"}
	for idx, column := range columns {
		this.Require().Equal(expectedColumnNames[idx], column.Name)
	}

	columns, err = tables.TableColumns(testhelpers.TestSchemaName, this.tablenames[1])
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), this.tablenames[1]+" does not exist")
}

func (this *TableSchemaCacheTestSuite) TestAllTableNames() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		nil,
	)
	this.Require().Nil(err)

	tablesList := tables.AllTableNames()
	for _, table := range this.tablenames {
		this.Require().Contains(tablesList, fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, table))
	}
}

func (this *TableSchemaCacheTestSuite) TestAllTableNamesEmpty() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		map[string]bool{"applicableByDefault": false},
	)
	this.Require().Nil(err)
	this.Require().Equal(ghostferry.TableSchemaCache{}, tables)

	this.Require().Equal(0, len(tables.AllTableNames()))
	this.Require().Nil(tables.AllTableNames())
}

func (this *TableSchemaCacheTestSuite) TestAsSlice() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		nil,
	)
	this.Require().Nil(err)

	tablesSlice := tables.AsSlice()

	this.Require().Equal(len(this.tablenames), len(tablesSlice))
	for _, table := range tablesSlice {
		this.Require().Contains(this.tablenames, table.Name)
	}
}

func (this *TableSchemaCacheTestSuite) TestAsSliceEmpty() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		map[string]bool{testhelpers.TestSchemaName: true},
		map[string]bool{"applicableByDefault": false},
	)
	this.Require().Nil(err)
	this.Require().Equal(ghostferry.TableSchemaCache{}, tables)
	this.Require().Equal(0, len(tables.AsSlice()))
	this.Require().Nil(tables.AsSlice())
}

func TestTableSchemaCache(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &TableSchemaCacheTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
