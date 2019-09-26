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
	tablenames  []string
	tableFilter *testhelpers.TestTableFilter
}

var ()

func (this *TableSchemaCacheTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	this.tablenames = []string{"test_table_1", "test_table_2", "test_table_3"}
	for _, tablename := range this.tablenames {
		testhelpers.SeedInitialData(this.Ferry.SourceDB, testhelpers.TestSchemaName, tablename, 0)
	}

	this.tableFilter = &testhelpers.TestTableFilter{
		DbsFunc:    testhelpers.DbApplicabilityFilter([]string{testhelpers.TestSchemaName}),
		TablesFunc: nil,
	}
}

func (this *TableSchemaCacheTestSuite) TearDownTest() {
	this.GhostferryUnitTestSuite.TearDownTest()
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesWithoutFiltering() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		this.tableFilter,
		nil,
	)

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

func (this *TableSchemaCacheTestSuite) TestLoadTablesRejectTablesWithoutNumericPK() {
	query := fmt.Sprintf("CREATE TABLE %s.%s (id varchar(20) not null, data TEXT, primary key(id))", testhelpers.TestSchemaName, "test_table_4")
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	_, err = ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil)

	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "non-numeric primary key")
	this.Require().Contains(err.Error(), "test_table_4")
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesRejectTablesWithoutAnyPK() {
	query := fmt.Sprintf("CREATE TABLE %s.%s (id bigint(20) not null, data TEXT)", testhelpers.TestSchemaName, "test_table_4")
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	_, err = ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil)

	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "table test_table_4 has 0 primary key columns")
}

func (this *TableSchemaCacheTestSuite) TestAllTableNames() {
	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil)
	this.Require().Nil(err)

	tablesList := tables.AllTableNames()
	for _, table := range this.tablenames {
		this.Require().Contains(tablesList, fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, table))
	}
}

func (this *TableSchemaCacheTestSuite) TestAllTableNamesEmpty() {
	tableFilter := &testhelpers.TestTableFilter{
		DbsFunc:    testhelpers.DbApplicabilityFilter([]string{testhelpers.TestSchemaName}),
		TablesFunc: func(tables []*ghostferry.TableSchema) []*ghostferry.TableSchema { return []*ghostferry.TableSchema{} },
	}

	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, tableFilter, nil)

	this.Require().Nil(err)
	this.Require().Equal(ghostferry.TableSchemaCache{}, tables)

	this.Require().Equal(0, len(tables.AllTableNames()))
	this.Require().Nil(tables.AllTableNames())
}

func (this *TableSchemaCacheTestSuite) TestAsSlice() {
	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil)
	this.Require().Nil(err)

	tablesSlice := tables.AsSlice()

	this.Require().Equal(len(this.tablenames), len(tablesSlice))
	for _, table := range tablesSlice {
		this.Require().Contains(this.tablenames, table.Name)
	}
}

func (this *TableSchemaCacheTestSuite) TestAsSliceEmpty() {
	tableFilter := &testhelpers.TestTableFilter{
		DbsFunc:    testhelpers.DbApplicabilityFilter([]string{testhelpers.TestSchemaName}),
		TablesFunc: func(tables []*ghostferry.TableSchema) []*ghostferry.TableSchema { return []*ghostferry.TableSchema{} },
	}

	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, tableFilter, nil)

	this.Require().Nil(err)
	this.Require().Equal(ghostferry.TableSchemaCache{}, tables)
	this.Require().Equal(0, len(tables.AsSlice()))
	this.Require().Nil(tables.AsSlice())
}

func (this *TableSchemaCacheTestSuite) TestFingerprintQuery() {
	tableSchemaCache, err := ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil)
	this.Require().Nil(err)

	tables := tableSchemaCache.AsSlice()
	table := tables[0]
	query := table.FingerprintQuery("s", "t", 10)
	this.Require().Equal("SELECT `id`,MD5(CONCAT(MD5(COALESCE(`id`, 'NULL_PBj}b]74P@JTo$5G_null')),MD5(COALESCE(`data`, 'NULL_PBj}b]74P@JTo$5G_null')))) AS __ghostferry_row_md5 FROM `s`.`t` WHERE `id` IN (?,?,?,?,?,?,?,?,?,?)", query)

	table = tables[1]
	table.CompressedColumns = map[string]string{"data": "SNAPPY"}
	query = table.FingerprintQuery("s", "t", 10)
	this.Require().Equal("SELECT `id`,MD5(CONCAT(MD5(COALESCE(`id`, 'NULL_PBj}b]74P@JTo$5G_null')))) AS __ghostferry_row_md5,`data` FROM `s`.`t` WHERE `id` IN (?,?,?,?,?,?,?,?,?,?)", query)
}

func (this *TableSchemaCacheTestSuite) TestTableRowMd5Query() {
	tableSchemaCache, err := ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil)
	this.Require().Nil(err)

	tables := tableSchemaCache.AsSlice()
	table := tables[0]
	query := table.RowMd5Query()
	this.Require().Equal("MD5(CONCAT(MD5(COALESCE(`id`, 'NULL_PBj}b]74P@JTo$5G_null')),MD5(COALESCE(`data`, 'NULL_PBj}b]74P@JTo$5G_null')))) AS __ghostferry_row_md5", query)

	table = tables[1]
	table.CompressedColumns = map[string]string{"data": "SNAPPY"}
	query = table.RowMd5Query()
	this.Require().Equal("MD5(CONCAT(MD5(COALESCE(`id`, 'NULL_PBj}b]74P@JTo$5G_null')))) AS __ghostferry_row_md5", query)
}

func (this *TableSchemaCacheTestSuite) TestQuotedTableName() {
	table := &ghostferry.TableSchema{
		Table: &sqlSchema.Table{
			Schema: "schema",
			Name:   "table",
		},
	}
	this.Require().Equal("`schema`.`table`", ghostferry.QuotedTableName(table))
}

func (this *TableSchemaCacheTestSuite) TestQuotedTableNameFromString() {
	this.Require().Equal("``.`table`", ghostferry.QuotedTableNameFromString("", "table"))
	this.Require().Equal("`schema`.`table`", ghostferry.QuotedTableNameFromString("schema", "table"))
	this.Require().Equal("`schema`.``", ghostferry.QuotedTableNameFromString("schema", ""))
	this.Require().Equal("``.``", ghostferry.QuotedTableNameFromString("", ""))
}

func TestTableSchemaCache(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &TableSchemaCacheTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
