package test

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/suite"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
)

const (
	TestDB1      = "gftest2"
	TestDB2      = "gftest3"
	TestDB3      = "gftest4"
	TestTableDB1 = "test_db_2"
	TestTableDB2 = "test_db_3"
	TestTableDB3 = "test_db_4"
)

var TestDBs = []string{TestDB1, TestDB2, TestDB3}

var DBTableMap = map[string]string{
	TestDB1: TestTableDB1,
	TestDB2: TestTableDB2,
	TestDB3: TestTableDB3,
}

type DataIteratorSorterTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	unsortedTables map[*ghostferry.TableSchema]ghostferry.PaginationKey
	dataIterator   *ghostferry.DataIterator
}

func (t *DataIteratorSorterTestSuite) SetupTest() {
	t.GhostferryUnitTestSuite.SetupTest()
	testhelpers.SeedInitialData(t.Ferry.SourceDB, TestDB1, TestTableDB1, 300)
	testhelpers.SeedInitialData(t.Ferry.SourceDB, TestDB2, TestTableDB2, 1)
	testhelpers.SeedInitialData(t.Ferry.SourceDB, TestDB3, TestTableDB3, 500)

	tableFilter := &testhelpers.TestTableFilter{
		DbsFunc:    testhelpers.DbApplicabilityFilter(TestDBs),
		TablesFunc: nil,
	}
	tables, _ := ghostferry.LoadTables(t.Ferry.SourceDB, tableFilter, nil, nil, nil, nil)

	t.unsortedTables = make(map[*ghostferry.TableSchema]ghostferry.PaginationKey, len(tables))
	i := 0
	for _, f := range tables.AsSlice() {
		maxPaginationKey := uint64(100_000 - i)
		t.unsortedTables[f] = ghostferry.NewUint64Key(maxPaginationKey)
		i++
	}

	t.dataIterator = &ghostferry.DataIterator{
		DB:                   t.Ferry.SourceDB,
		ErrorHandler:         t.Ferry.ErrorHandler,
		TargetPaginationKeys: &sync.Map{},
	}
}

func (t *DataIteratorSorterTestSuite) TearDownTest() {
	for _, db := range TestDBs {
		_, err := t.Ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", db))
		t.Require().Nil(err)
	}
}

func (t *DataIteratorSorterTestSuite) TestOrderMaxPaginationKeys() {
	sorter := ghostferry.MaxPaginationKeySorter{}

	sortedTables, err := sorter.Sort(t.unsortedTables)

	if err != nil {
		t.Fail("Could not sort tables for data iterator using MaxPaginationKeySorter")
	}

	expectedTables := make([]ghostferry.TableMaxPaginationKey, len(sortedTables))
	copy(expectedTables, sortedTables)

	sort.Slice(expectedTables, func(i, j int) bool {
		return sortedTables[i].MaxPaginationKey.Compare(sortedTables[j].MaxPaginationKey) > 0
	})

	t.Require().Equal(len(t.unsortedTables), len(sortedTables))
	t.Require().EqualValues(expectedTables, sortedTables)

}

func (t *DataIteratorSorterTestSuite) TestOrderMaxPaginationKeysMixedTypes() {
	sorter := ghostferry.MaxPaginationKeySorter{}

	// Create a mix of keys: Uint64Key, BinaryKey, CompositeKey
	mixedTables := map[*ghostferry.TableSchema]ghostferry.PaginationKey{
		{Table: &schema.Table{Schema: "test", Name: "uint_table"}}:      ghostferry.NewUint64Key(100),
		{Table: &schema.Table{Schema: "test", Name: "binary_table"}}:    ghostferry.NewBinaryKey([]byte("abc")),
		{Table: &schema.Table{Schema: "test", Name: "composite_table"}}: ghostferry.CompositeKey{ghostferry.NewUint64Key(1)},
	}

	// This should not panic
	sortedTables, err := sorter.Sort(mixedTables)
	t.Require().Nil(err)
	t.Require().Equal(3, len(sortedTables))

	// Verify we have one of each
	typesSeen := make(map[string]bool)
	for _, item := range sortedTables {
		typesSeen[fmt.Sprintf("%T", item.MaxPaginationKey)] = true
	}
	t.Require().True(typesSeen["ghostferry.Uint64Key"])
	t.Require().True(typesSeen["ghostferry.BinaryKey"])
	t.Require().True(typesSeen["ghostferry.CompositeKey"])
}

func (t *DataIteratorSorterTestSuite) TestOrderByInformationSchemaTableSize() {

	// information_schemas.table does not update automatically on every write
	// ANALYZE TABLE will trigger an update so we can get the latest db sizes
	for schema, table := range DBTableMap {
		_, err := t.Ferry.SourceDB.Exec(fmt.Sprintf("USE %s", schema))

		if err != nil {
			t.Fail("Could not Update information_schemas.tables to get latest table sizes")
		}
		_, err = t.Ferry.SourceDB.Exec(fmt.Sprintf("OPTIMIZE TABLE `%s`", table))
		if err != nil {
			t.Fail("Could not Update information_schemas.tables to get latest table sizes")
		}
	}

	sorter := ghostferry.MaxTableSizeSorter{DataIterator: t.Ferry.DataIterator}

	sortedTables, err := sorter.Sort(t.unsortedTables)

	if err != nil {
		t.Fail("Could not sort tables for data iterator using MaxTableSizeSorter")
	}

	t.Require().Equal(len(t.unsortedTables), 3)

	table1 := sortedTables[0].Table
	table2 := sortedTables[1].Table
	table3 := sortedTables[2].Table
	t.Require().Equal("gftest4 test_db_4", fmt.Sprintf("%s %s", table1.Schema, table1.Name))
	t.Require().Equal("gftest2 test_db_2", fmt.Sprintf("%s %s", table2.Schema, table2.Name))
	t.Require().Equal("gftest3 test_db_3", fmt.Sprintf("%s %s", table3.Schema, table3.Name))
}

func TestDataIteratorSorterTestSuite(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &DataIteratorSorterTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
