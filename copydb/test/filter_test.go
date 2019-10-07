package test

import (
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/copydb"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"

	sqlSchema "github.com/siddontang/go-mysql/schema"
)

type FilterTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite
	tablenames []string
}

func (this *FilterTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	this.tablenames = []string{"test_table_1", "test_table_2", "test_table_3"}
	for _, tablename := range this.tablenames {
		testhelpers.SeedInitialData(this.Ferry.SourceDB, testhelpers.TestSchemaName, tablename, 0)
	}
}

func (this *FilterTestSuite) TestLoadTablesWithWhitelist() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		copydb.NewStaticTableFilter(
			copydb.FilterAndRewriteConfigs{
				Whitelist: []string{testhelpers.TestSchemaName},
			},
			copydb.FilterAndRewriteConfigs{
				Whitelist: []string{"test_table_2"},
			},
		),
		nil,
		nil,
		nil,
	)

	this.Require().Nil(err)
	this.Require().Equal(1, len(tables))

	_, exists := tables[fmt.Sprintf("%s.test_table_2", testhelpers.TestSchemaName)]
	this.Require().True(exists)

	_, exists = tables[fmt.Sprintf("%s.test_table_3", testhelpers.TestSchemaName)]
	this.Require().False(exists)
}

func (this *FilterTestSuite) TestLoadTablesWithBlacklist() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		copydb.NewStaticTableFilter(
			copydb.FilterAndRewriteConfigs{
				Whitelist: []string{testhelpers.TestSchemaName},
			},
			copydb.FilterAndRewriteConfigs{
				Blacklist: []string{"test_table_2"},
			},
		),
		nil,
		nil,
		nil,
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

func (this *FilterTestSuite) TestFilterWithSimpleWhitelist() {
	list := []string{"str1", "str2", "str3"}
	filter := copydb.FilterAndRewriteConfigs{
		Whitelist: []string{"str1"},
	}

	this.assertBothFilters([]string{"str1"}, filter, list)
}

func (this *FilterTestSuite) TestFilterWithNonExistentKeyInWhitelist() {
	list := []string{"str1", "str2", "str3"}
	filter := copydb.FilterAndRewriteConfigs{
		Whitelist: []string{"str1", "nonexistent"},
	}

	this.assertBothFilters([]string{"str1"}, filter, list)
}

func (this *FilterTestSuite) TestFilterWithSimpleBlacklist() {
	list := []string{"str1", "str2", "str3"}

	filter := copydb.FilterAndRewriteConfigs{
		Blacklist: []string{"str1"},
	}

	this.assertBothFilters([]string{"str2", "str3"}, filter, list)
}

func (this *FilterTestSuite) TestFilterWithEmptyFilter() {
	list := []string{"str1", "str2", "str3"}
	filter := copydb.FilterAndRewriteConfigs{}
	this.assertBothFilters(list, filter, list)
}

func (this *FilterTestSuite) TestFilterForEmptyList() {
	list := []string{}
	filter := copydb.FilterAndRewriteConfigs{}
	this.assertBothFilters(list, filter, list)
}

func (this *FilterTestSuite) assertBothFilters(expected []string, filter copydb.FilterAndRewriteConfigs, list []string) {
	tableFilter := copydb.NewStaticTableFilter(filter, filter)

	actual, err := tableFilter.ApplicableDatabases(list)
	this.Require().Nil(err)
	this.Require().Equal(expected, actual)

	var schemas []*ghostferry.TableSchema
	for _, table := range list {
		schemas = append(schemas, &ghostferry.TableSchema{
			Table: &sqlSchema.Table{Name: table},
		})
	}

	filtered, err := tableFilter.ApplicableTables(schemas)
	this.Require().Nil(err)

	applicableTables := []string{}
	for _, table := range filtered {
		applicableTables = append(applicableTables, table.Name)
	}

	this.Require().Equal(expected, applicableTables)
}

func TestFilter(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &FilterTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
