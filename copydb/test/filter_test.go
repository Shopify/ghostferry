package test

import (
	"testing"

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

func (this *FilterTestSuite) TestFilterForApplicableNil() {
	list := []string{"str1", "str2", "str3"}
	this.assertBothFilters(list, nil, list)
}

func (this *FilterTestSuite) TestFilterForApplicableSimple() {
	list := []string{"str1", "str2", "str3"}
	filter := map[string]bool{
		"str1": true,
		"str2": false,
	}

	this.assertBothFilters([]string{"str1"}, filter, list)
}

func (this *FilterTestSuite) TestFilterForApplicableNonExistentKey() {
	list := []string{"str1", "str2", "str3"}
	filter := map[string]bool{
		"str1":        true,
		"nonexistent": true,
	}

	this.assertBothFilters([]string{"str1"}, filter, list)
}

func (this *FilterTestSuite) TestFilterForApplicableDefaultFalse() {
	list := []string{"str1", "str2", "str3"}
	filter := map[string]bool{
		"ApplicableByDefault!": false,
	}

	this.assertBothFilters([]string{}, filter, list)
}

func (this *FilterTestSuite) TestFilterForApplicableDefaultTrue() {
	list := []string{"str1", "str2", "str3"}
	filter := map[string]bool{
		"ApplicableByDefault!": true,
	}
	this.assertBothFilters(list, filter, list)
}

func (this *FilterTestSuite) TestFilterForApplicableEmptyList() {
	list := []string{}
	filter := map[string]bool{
		"ApplicableByDefault!": true,
	}
	this.assertBothFilters(list, filter, list)
}

func (this *FilterTestSuite) assertBothFilters(expected []string, filter map[string]bool, list []string) {
	applicability := copydb.NewStaticApplicableFilter(filter, filter)

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

func TestFilter(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &FilterTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
