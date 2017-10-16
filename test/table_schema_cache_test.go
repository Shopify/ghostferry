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
		testhelpers.SeedInitialData(this.Ferry.SourceDB, testhelpers.TestSchemaName, tablename, 0)
	}
}

func (this *TableSchemaCacheTestSuite) TearDownTest() {
	this.GhostferryUnitTestSuite.TearDownTest()
}

func SimpleApplicability(applicableDbs, applicableTables map[string]bool) *ghostferry.SimpleApplicableFilter {
	return &ghostferry.SimpleApplicableFilter{
		Dbs:    applicableDbs,
		Tables: applicableTables,
	}
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesWithoutFiltering() {
	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, SimpleApplicability(map[string]bool{testhelpers.TestSchemaName: true}, nil))
	this.Require().Nil(err)
	this.Require().Equal(len(this.tablenames), len(tables))
	for _, tablename := range this.tablenames {
		schema := tables[fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, tablename)]

		this.Require().Equal(testhelpers.TestSchemaName, schema.Schema)
		this.Require().Equal(tablename, schema.Name)
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
		SimpleApplicability(
			map[string]bool{testhelpers.TestSchemaName: true},
			map[string]bool{"test_table_2": true},
		),
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
		SimpleApplicability(
			map[string]bool{testhelpers.TestSchemaName: true},
			map[string]bool{"test_table_2": false, "ApplicableByDefault!": true},
		),
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
		SimpleApplicability(
			map[string]bool{testhelpers.TestSchemaName: true},
			nil,
		),
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
		SimpleApplicability(
			map[string]bool{testhelpers.TestSchemaName: true},
			nil,
		),
	)

	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "table test_table_4 has 0 primary key columns")
}

func (this *TableSchemaCacheTestSuite) TestAllTableNames() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		SimpleApplicability(
			map[string]bool{testhelpers.TestSchemaName: true},
			nil,
		),
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
		SimpleApplicability(
			map[string]bool{testhelpers.TestSchemaName: true},
			map[string]bool{"ApplicableByDefault!": false},
		),
	)
	this.Require().Nil(err)
	this.Require().Equal(ghostferry.TableSchemaCache{}, tables)

	this.Require().Equal(0, len(tables.AllTableNames()))
	this.Require().Nil(tables.AllTableNames())
}

func (this *TableSchemaCacheTestSuite) TestAsSlice() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		SimpleApplicability(
			map[string]bool{testhelpers.TestSchemaName: true},
			nil,
		),
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
		SimpleApplicability(
			map[string]bool{testhelpers.TestSchemaName: true},
			map[string]bool{"ApplicableByDefault!": false},
		),
	)
	this.Require().Nil(err)
	this.Require().Equal(ghostferry.TableSchemaCache{}, tables)
	this.Require().Equal(0, len(tables.AsSlice()))
	this.Require().Nil(tables.AsSlice())
}

func (this *TableSchemaCacheTestSuite) TestQuotedTableName() {
	table := &sqlSchema.Table{}
	this.Require().Equal("``.``", ghostferry.QuotedTableName(table))

	table = &sqlSchema.Table{
		Schema: "schema",
		Name:   "table",
	}
	this.Require().Equal("`schema`.`table`", ghostferry.QuotedTableName(table))
}

func (this *TableSchemaCacheTestSuite) TestQuotedTableNameFromString() {
	this.Require().Equal("``.`table`", ghostferry.QuotedTableNameFromString("", "table"))
	this.Require().Equal("`schema`.`table`", ghostferry.QuotedTableNameFromString("schema", "table"))
	this.Require().Equal("`schema`.``", ghostferry.QuotedTableNameFromString("schema", ""))
	this.Require().Equal("``.``", ghostferry.QuotedTableNameFromString("", ""))
}

func (this *TableSchemaCacheTestSuite) TestFilterForApplicableNil() {
	list := []string{"str1", "str2", "str3"}
	this.assertBothFilters(list, nil, list)
}

func (this *TableSchemaCacheTestSuite) TestFilterForApplicableSimple() {
	list := []string{"str1", "str2", "str3"}
	filter := map[string]bool{
		"str1": true,
		"str2": false,
	}

	this.assertBothFilters([]string{"str1"}, filter, list)
}

func (this *TableSchemaCacheTestSuite) TestFilterForApplicableNonExistentKey() {
	list := []string{"str1", "str2", "str3"}
	filter := map[string]bool{
		"str1":        true,
		"nonexistent": true,
	}

	this.assertBothFilters([]string{"str1"}, filter, list)
}

func (this *TableSchemaCacheTestSuite) TestFilterForApplicableDefaultFalse() {
	list := []string{"str1", "str2", "str3"}
	filter := map[string]bool{
		"ApplicableByDefault!": false,
	}

	this.assertBothFilters([]string{}, filter, list)
}

func (this *TableSchemaCacheTestSuite) TestFilterForApplicableDefaultTrue() {
	list := []string{"str1", "str2", "str3"}
	filter := map[string]bool{
		"ApplicableByDefault!": true,
	}
	this.assertBothFilters(list, filter, list)
}

func (this *TableSchemaCacheTestSuite) TestFilterForApplicableEmptyList() {
	list := []string{}
	filter := map[string]bool{
		"ApplicableByDefault!": true,
	}
	this.assertBothFilters(list, filter, list)
}

func (this *TableSchemaCacheTestSuite) assertBothFilters(expected []string, filter map[string]bool, list []string) {
	applicability := SimpleApplicability(filter, filter)

	this.Require().Equal(expected, applicability.ApplicableDbs(list))

	var schemas []*sqlSchema.Table
	for _, table := range list {
		schemas = append(schemas, &sqlSchema.Table{Name: table})
	}

	applicableTables := []string{}
	for _, table := range applicability.ApplicableTables(schemas) {
		applicableTables = append(applicableTables, table.Name)
	}

	this.Require().Equal(expected, applicableTables)
}

func TestTableSchemaCache(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &TableSchemaCacheTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
