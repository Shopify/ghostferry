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

func dropTestTables(this *TableSchemaCacheTestSuite) {
	var query string
	var err error
	for _, tablename := range this.tablenames {
		query = fmt.Sprintf("DROP TABLE %s.%s", testhelpers.TestSchemaName, tablename)
		_, err = this.Ferry.SourceDB.Exec(query)
		this.Require().Nil(err)
	}
}

func (t *TableSchemaCacheTestSuite) assertLoadTablesWithCascadingPaginationColumnConfig(table, expectedpaginationColumn string, cascadingPaginationColumnConfig *ghostferry.CascadingPaginationColumnConfig) {
	tableSchemaCache, err := ghostferry.LoadTables(t.Ferry.SourceDB, t.tableFilter, nil, nil, cascadingPaginationColumnConfig)
	actual := tableSchemaCache.Get(testhelpers.TestSchemaName, table).PaginationKeyColumn.Name
	t.Require().Equal(expectedpaginationColumn, actual)
	t.Require().Nil(err)
}

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
		nil,
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
	table := "test_table_4"
	paginationColumn := "id"
	query := fmt.Sprintf("CREATE TABLE %s.%s (%s varchar(20) not null, data TEXT, primary key(%s))", testhelpers.TestSchemaName, table, paginationColumn, paginationColumn)
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	_, err = ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, nil)

	this.Require().NotNil(err)
	this.Require().EqualError(err, ghostferry.NonNumericPaginationKeyError(testhelpers.TestSchemaName, table, paginationColumn).Error())
	this.Require().Contains(err.Error(), table)
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesCascadingPaginationColumnConfigRightScenario1() {
	table := "pagination_by_column_config_right_scenario_1"
	paginationColumn := "identity"
	cascadingPaginationColumnConfig := &ghostferry.CascadingPaginationColumnConfig{
		PerTable: map[string]map[string]string{
			testhelpers.TestSchemaName: map[string]string{
				table: paginationColumn,
			},
		},
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (%s bigint(20) not null, data TEXT)", testhelpers.TestSchemaName, table, paginationColumn)
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)
	this.assertLoadTablesWithCascadingPaginationColumnConfig(table, paginationColumn, cascadingPaginationColumnConfig)
}
func (this *TableSchemaCacheTestSuite) TestLoadTablesCascadingPaginationColumnConfigRightScenario2() {
	dropTestTables(this) // needed because the default tables created at test setup interfere with this test

	table := "pagination_by_column_config_right_scenario_2"
	fallbackColumn := "identity"
	cascadingPaginationColumnConfig := &ghostferry.CascadingPaginationColumnConfig{
		FallbackColumn: fallbackColumn,
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (%s bigint(20) not null, data TEXT)", testhelpers.TestSchemaName, table, fallbackColumn)
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	this.assertLoadTablesWithCascadingPaginationColumnConfig(table, fallbackColumn, cascadingPaginationColumnConfig)
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesCascadingPaginationColumnConfigUsesPKBeforeFallback() {
	dropTestTables(this) // needed because the default tables created at test setup interfere with this test

	table := "pagination_by_column_config_uses_pk_before_fallback"
	fallbackColumn := "non_existent"
	cascadingPaginationColumnConfig := &ghostferry.CascadingPaginationColumnConfig{
		FallbackColumn: fallbackColumn,
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (id bigint(20), %s bigint(20) not null, data TEXT, primary key(id))", testhelpers.TestSchemaName, table, fallbackColumn)
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	this.assertLoadTablesWithCascadingPaginationColumnConfig(table, "id", cascadingPaginationColumnConfig)
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesRejectTablesWhenCascadingPaginationColumnConfigWrong() {
	table := "pagination_by_column_config_wrong_scenario_1"
	paginationColumn := "non_existent"
	cascadingPaginationColumnConfig := &ghostferry.CascadingPaginationColumnConfig{
		PerTable: map[string]map[string]string{
			testhelpers.TestSchemaName: map[string]string{
				table: paginationColumn,
			},
		},
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (id bigint(20) not null, data TEXT)", testhelpers.TestSchemaName, table)
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	_, err = ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, cascadingPaginationColumnConfig)

	this.Require().NotNil(err)
	this.Require().EqualError(err, ghostferry.NonExistingPaginationKeyColumnError(testhelpers.TestSchemaName, table, paginationColumn).Error())
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesRejectTablesWhenCascadingPaginationColumnConfigWrongScenario2() {
	table := "pagination_by_column_config_wrong_scenario_2"
	paginationColumn := "non_existent"
	cascadingPaginationColumnConfig := &ghostferry.CascadingPaginationColumnConfig{
		FallbackColumn: paginationColumn,
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (id bigint(20) not null, data TEXT)", testhelpers.TestSchemaName, table)
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	_, err = ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, cascadingPaginationColumnConfig)

	this.Require().NotNil(err)
	this.Require().EqualError(err, ghostferry.NonExistingPaginationKeyColumnError(testhelpers.TestSchemaName, table, paginationColumn).Error())
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesWithPaginationKeyColumnFallback() {
	table := "pk_fallback_column_present"
	query := fmt.Sprintf("CREATE TABLE %s.%s (identity bigint(20) not null, data TEXT, primary key(identity))", testhelpers.TestSchemaName, table)
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	_, err = ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, nil)

	this.Require().Nil(err)
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesRejectTablesWithoutPKColumnToFallBackTo() {
	table := "pk_fallback_column_absent"
	query := fmt.Sprintf("CREATE TABLE %s.%s (identity bigint(20) not null, data TEXT)", testhelpers.TestSchemaName, table)
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	_, err = ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, nil)

	this.Require().NotNil(err)
	this.Require().EqualError(err, ghostferry.NonExistingPaginationKeyError(testhelpers.TestSchemaName, table).Error())
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesRejectTablesWithCompositePKButNoAlternateColumnToFallBackTo() {
	table := "composite_pk_without_fallback"
	query := fmt.Sprintf("CREATE TABLE %s.%s (identity bigint(20) not null, other_id bigint(20) not null, data TEXT, primary key(identity, other_id))", testhelpers.TestSchemaName, table)
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	_, err = ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, nil)

	this.Require().NotNil(err)
	this.Require().EqualError(err, ghostferry.NonExistingPaginationKeyError(testhelpers.TestSchemaName, table).Error())
}

func (this *TableSchemaCacheTestSuite) TestLoadTablesWithCompositePKButIDColumnToFallBackTo() {
	table := "composite_pk_with_id_fallback"
	paginationColumn := "id"
	cascadingPaginationColumnConfig := &ghostferry.CascadingPaginationColumnConfig{
		PerTable: map[string]map[string]string{
			testhelpers.TestSchemaName: map[string]string{
				table: paginationColumn,
			},
		},
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (identity bigint(20) not null, id bigint(20) not null, data TEXT, primary key(identity, id))", testhelpers.TestSchemaName, table)
	_, err := this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	this.assertLoadTablesWithCascadingPaginationColumnConfig(table, paginationColumn, cascadingPaginationColumnConfig)

	cascadingPaginationColumnConfig = &ghostferry.CascadingPaginationColumnConfig{
		FallbackColumn: paginationColumn,
	}
	this.assertLoadTablesWithCascadingPaginationColumnConfig(table, paginationColumn, cascadingPaginationColumnConfig)
}

func (this *TableSchemaCacheTestSuite) TestAllTableNames() {
	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, nil)
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

	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, tableFilter, nil, nil, nil)

	this.Require().Nil(err)
	this.Require().Equal(ghostferry.TableSchemaCache{}, tables)

	this.Require().Equal(0, len(tables.AllTableNames()))
	this.Require().Nil(tables.AllTableNames())
}

func (this *TableSchemaCacheTestSuite) TestAsSlice() {
	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, nil)
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

	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, tableFilter, nil, nil, nil)

	this.Require().Nil(err)
	this.Require().Equal(ghostferry.TableSchemaCache{}, tables)
	this.Require().Equal(0, len(tables.AsSlice()))
	this.Require().Nil(tables.AsSlice())
}

func (this *TableSchemaCacheTestSuite) TestFingerprintQuery() {
	tableSchemaCache, err := ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, nil)
	this.Require().Nil(err)

	tables := tableSchemaCache.AsSlice()
	table := tables[0]
	query := table.FingerprintQuery("s", "t", 10)
	this.Require().Equal("SELECT `id`,MD5(CONCAT(MD5(COALESCE(`id`, 'NULL_PBj}b]74P@JTo$5G_null')),MD5(COALESCE(`data`, 'NULL_PBj}b]74P@JTo$5G_null')))) AS __ghostferry_row_md5 FROM `s`.`t` WHERE `id` IN (?,?,?,?,?,?,?,?,?,?)", query)

	table = tables[1]
	table.CompressedColumnsForVerification = map[string]string{"data": "SNAPPY"}
	query = table.FingerprintQuery("s", "t", 10)
	this.Require().Equal("SELECT `id`,MD5(CONCAT(MD5(COALESCE(`id`, 'NULL_PBj}b]74P@JTo$5G_null')))) AS __ghostferry_row_md5,`data` FROM `s`.`t` WHERE `id` IN (?,?,?,?,?,?,?,?,?,?)", query)
}

func (this *TableSchemaCacheTestSuite) TestTableRowMd5Query() {
	tableSchemaCache, err := ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, nil)
	this.Require().Nil(err)

	tables := tableSchemaCache.AsSlice()
	table := tables[0]
	query := table.RowMd5Query()
	this.Require().Equal("MD5(CONCAT(MD5(COALESCE(`id`, 'NULL_PBj}b]74P@JTo$5G_null')),MD5(COALESCE(`data`, 'NULL_PBj}b]74P@JTo$5G_null')))) AS __ghostferry_row_md5", query)

	table = tables[1]
	table.CompressedColumnsForVerification = map[string]string{"data": "SNAPPY"}
	query = table.RowMd5Query()
	this.Require().Equal("MD5(CONCAT(MD5(COALESCE(`id`, 'NULL_PBj}b]74P@JTo$5G_null')))) AS __ghostferry_row_md5", query)
}

func (this *TableSchemaCacheTestSuite) TestFingerprintQueryWithIgnoredColumns() {
	tableSchemaCache, err := ghostferry.LoadTables(this.Ferry.SourceDB, this.tableFilter, nil, nil, nil)
	this.Require().Nil(err)

	tables := tableSchemaCache.AsSlice()
	table := tables[0]
	table.IgnoredColumnsForVerification = map[string]struct{}{
		"data": struct{}{},
	}
	query := table.FingerprintQuery("s", "t", 10)
	this.Require().Equal("SELECT `id`,MD5(CONCAT(MD5(COALESCE(`id`, 'NULL_PBj}b]74P@JTo$5G_null')))) AS __ghostferry_row_md5 FROM `s`.`t` WHERE `id` IN (?,?,?,?,?,?,?,?,?,?)", query)
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

func getMultiTableMap() *ghostferry.TableSchemaCache {
	return &ghostferry.TableSchemaCache{
		"schema.table1": &ghostferry.TableSchema{
			Table: &sqlSchema.Table{
				Schema: "schema",
				Name:   "table1",
			},
		},
		"schema.table2": &ghostferry.TableSchema{
			Table: &sqlSchema.Table{
				Schema: "schema",
				Name:   "table2",
			},
		},
		"schema.table3": &ghostferry.TableSchema{
			Table: &sqlSchema.Table{
				Schema: "schema",
				Name:   "table3",
			},
		},
	}
}

func (this *TableSchemaCacheTestSuite) TestGetTableListWithPriorityNil() {
	tables := getMultiTableMap()
	// make sure we are not losing any elements, even if the priority does not
	// mater
	creationOrder := tables.GetTableListWithPriority(nil)
	this.Require().Equal(len(creationOrder), 3)
	this.Require().ElementsMatch(creationOrder, tables.AllTableNames())
}

func (this *TableSchemaCacheTestSuite) TestGetTableListWithPriority() {
	tables := getMultiTableMap()
	creationOrder := tables.GetTableListWithPriority([]string{"schema.table2"})
	this.Require().Equal(len(creationOrder), 3)
	this.Require().ElementsMatch(creationOrder, tables.AllTableNames())
	this.Require().Equal(creationOrder[0], "schema.table2")
}

func (this *TableSchemaCacheTestSuite) TestGetTableListWithPriorityIgnoreUnknown() {
	tables := getMultiTableMap()
	creationOrder := tables.GetTableListWithPriority([]string{"schema.table2", "schema.unknown_table"})
	this.Require().Equal(len(creationOrder), 3)
	this.Require().ElementsMatch(creationOrder, tables.AllTableNames())
	this.Require().Equal(creationOrder[0], "schema.table2")
}

func TestTableSchemaCache(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &TableSchemaCacheTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
