package test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
)

type DataIteratorTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	di           *ghostferry.DataIterator
	wg           *sync.WaitGroup
	receivedRows map[string][]ghostferry.RowData
}

func (this *DataIteratorTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()
	this.SeedSourceDB(5)

	sourceDb := this.Ferry.SourceDB
	config := this.Ferry.Config
	errorHandler := this.Ferry.ErrorHandler
	throttler := this.Ferry.Throttler

	tableFilter := &testhelpers.TestTableFilter{
		DbsFunc:    testhelpers.DbApplicabilityFilter([]string{testhelpers.TestSchemaName}),
		TablesFunc: nil,
	}

	tables, err := ghostferry.LoadTables(sourceDb, tableFilter)
	this.Require().Nil(err)

	config.DataIterationBatchSize = 2

	this.di = &ghostferry.DataIterator{
		DB:          sourceDb,
		Concurrency: config.DataIterationConcurrency,

		ErrorHandler: errorHandler,
		CursorConfig: &ghostferry.CursorConfig{
			DB:        sourceDb,
			Throttler: throttler,

			BuildSelect: nil,
			BatchSize:   config.DataIterationBatchSize,
			ReadRetries: config.DBReadRetries,
		},
		StateTracker: ghostferry.NewStateTracker(config.DataIterationConcurrency * 10),

		Tables: tables.AsSlice(),
	}

	this.receivedRows = make(map[string][]ghostferry.RowData, 0)

	this.di.Initialize()
	this.di.AddBatchListener(func(ev *ghostferry.RowBatch) error {
		this.receivedRows[ev.TableSchema().Name] = append(this.receivedRows[ev.TableSchema().Name], ev.Values()...)
		return nil
	})
}

func (this *DataIteratorTestSuite) TestNoEventsForEmptyTable() {
	_, err := this.Ferry.SourceDB.Query(fmt.Sprintf("DELETE FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestTable1Name))
	_, err = this.Ferry.SourceDB.Query(fmt.Sprintf("DELETE FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestCompressedTable1Name))
	this.Require().Nil(err)

	this.di.Run()

	this.Require().Equal(0, len(this.receivedRows))
	this.Require().Equal(
		this.completedTables(),
		map[string]bool{
			fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, testhelpers.TestTable1Name):           true,
			fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, testhelpers.TestCompressedTable1Name): true,
		},
	)
}

func (this *DataIteratorTestSuite) TestExistingRowsAreIterated() {
	ids := make(map[string][]int64)
	data := make(map[string][]string)

	rows, err := this.Ferry.SourceDB.Query(fmt.Sprintf("SELECT id, data FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestTable1Name))
	this.Require().Nil(err)
	defer rows.Close()

	for rows.Next() {
		var id int64
		var datum string

		err = rows.Scan(&id, &datum)
		this.Require().Nil(err)

		ids[testhelpers.TestTable1Name] = append(ids[testhelpers.TestTable1Name], id)
		data[testhelpers.TestTable1Name] = append(data[testhelpers.TestTable1Name], datum)
	}

	rows, err = this.Ferry.SourceDB.Query(fmt.Sprintf("SELECT id, data FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestCompressedTable1Name))
	this.Require().Nil(err)
	defer rows.Close()

	for rows.Next() {
		var id int64
		var datum string

		err = rows.Scan(&id, &datum)
		this.Require().Nil(err)

		ids[testhelpers.TestCompressedTable1Name] = append(ids[testhelpers.TestCompressedTable1Name], id)
		data[testhelpers.TestCompressedTable1Name] = append(data[testhelpers.TestCompressedTable1Name], datum)
	}

	this.Require().Equal(0, len(this.completedTables()))

	this.Require().Equal(5, len(ids[testhelpers.TestTable1Name]))
	this.Require().Equal(5, len(ids[testhelpers.TestCompressedTable1Name]))
	this.Require().Equal(0, len(this.receivedRows[testhelpers.TestTable1Name]))
	this.Require().Equal(0, len(this.receivedRows[testhelpers.TestCompressedTable1Name]))

	this.di.Run()

	this.Require().Equal(5, len(this.receivedRows[testhelpers.TestTable1Name]))
	this.Require().Equal(5, len(this.receivedRows[testhelpers.TestCompressedTable1Name]))

	for table, rows := range this.receivedRows {
		for idx, row := range rows {
			id := int64(row[0].(int64))
			datum := string(row[1].([]byte))

			this.Require().Equal(ids[table][idx], id)
			this.Require().Equal(data[table][idx], datum)
		}
	}

	this.Require().Equal(
		this.completedTables(),
		map[string]bool{
			fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, testhelpers.TestTable1Name):           true,
			fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, testhelpers.TestCompressedTable1Name): true,
		},
	)
}

func (this *DataIteratorTestSuite) TestDoneListenerGetsNotifiedWhenDone() {
	wasNotified := false

	this.di.AddDoneListener(func() error {
		wasNotified = true
		return nil
	})

	this.di.Run()

	this.Require().True(wasNotified)
}

func (this *DataIteratorTestSuite) completedTables() map[string]bool {
	return this.di.StateTracker.Serialize(nil).CompletedTables
}

func TestDataIterator(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &DataIteratorTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
