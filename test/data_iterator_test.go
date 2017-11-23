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

	di             *ghostferry.DataIterator
	wg             *sync.WaitGroup
	receivedEvents []ghostferry.DMLEvent
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

	config.IterateChunksize = 2

	this.di = &ghostferry.DataIterator{
		Db:           sourceDb,
		Config:       config,
		ErrorHandler: errorHandler,
		Throttler:    throttler,

		Tables: tables.AsSlice(),
	}

	this.receivedEvents = make([]ghostferry.DMLEvent, 0)

	this.di.Initialize()
	this.di.AddEventListener(func(ev []ghostferry.DMLEvent) error {
		this.receivedEvents = append(this.receivedEvents, ev...)
		return nil
	})
}

func (this *DataIteratorTestSuite) TestNoEventsForEmptyTable() {
	_, err := this.Ferry.SourceDB.Query(fmt.Sprintf("DELETE FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestTable1Name))
	this.Require().Nil(err)

	this.di.Run()

	this.Require().Equal(0, len(this.receivedEvents))
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

	this.di.Run()

	for idx, ev := range this.receivedEvents {
		this.Require().Equal(ev.NewValues(), ev.OldValues())

		id := int64(ev.NewValues()[0].(int64))
		data := string(ev.NewValues()[1].([]byte))

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
	di := &ghostferry.DataIterator{
		Config: this.Ferry.Config,
	}

	err := di.Initialize()
	this.Require().Nil(err)

	this.Require().NotNil(di.CurrentState)
}

func TestDataIterator(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &DataIteratorTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
