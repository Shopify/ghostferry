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
	receivedRows []ghostferry.RowData
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

		Tables: tables.AsSlice(),
	}

	this.receivedRows = make([]ghostferry.RowData, 0)

	this.di.Initialize()
	this.di.AddBatchListener(func(ev *ghostferry.RowBatch) error {
		this.receivedRows = append(this.receivedRows, ev.Values()...)
		return nil
	})
}

func (this *DataIteratorTestSuite) TestNoEventsForEmptyTable() {
	_, err := this.Ferry.SourceDB.Query(fmt.Sprintf("DELETE FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestTable1Name))
	this.Require().Nil(err)

	this.di.Run()

	this.Require().Equal(0, len(this.receivedRows))
	this.Require().Equal(this.di.CurrentState.CompletedTables(), map[string]bool{fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, testhelpers.TestTable1Name): true})
}

func (this *DataIteratorTestSuite) TestExistingRowsAreIterated() {
	var ids []int64
	var datas []string

	rows, err := this.Ferry.SourceDB.Query(fmt.Sprintf("SELECT id, data FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestTable1Name))
	this.Require().Nil(err)
	defer rows.Close()

	for rows.Next() {
		var id int64
		var data string

		err = rows.Scan(&id, &data)
		this.Require().Nil(err)

		ids = append(ids, id)
		datas = append(datas, data)
	}

	this.Require().Equal(0, len(this.di.CurrentState.CompletedTables()))

	this.Require().Equal(5, len(ids))
	this.Require().Equal(0, len(this.receivedRows))

	this.di.Run()

	this.Require().Equal(5, len(this.receivedRows))

	for idx, row := range this.receivedRows {
		id := int64(row[0].(int64))
		data := string(row[1].([]byte))

		this.Require().Equal(ids[idx], id)
		this.Require().Equal(datas[idx], data)
	}

	this.Require().Equal(this.di.CurrentState.CompletedTables(), map[string]bool{fmt.Sprintf("%s.%s", testhelpers.TestSchemaName, testhelpers.TestTable1Name): true})
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

func (this *DataIteratorTestSuite) TestInitialize() {
	this.Require().NotNil(this.di.CurrentState)
}

func TestDataIterator(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &DataIteratorTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
