package test

import (
	"fmt"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
)

type DataIteratorTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	di           *ghostferry.DataIterator
	wg           *sync.WaitGroup
	tables       []*ghostferry.TableSchema
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

	tables, err := ghostferry.LoadTables(sourceDb, tableFilter, nil, nil, nil, nil)
	this.Require().Nil(err)

	this.Ferry.Tables = tables
	this.tables = tables.AsSlice()

	config.DataIterationBatchSize = 2

	this.di = &ghostferry.DataIterator{
		DB:          sourceDb,
		Concurrency: config.DataIterationConcurrency,

		ErrorHandler: errorHandler,
		CursorConfig: &ghostferry.CursorConfig{
			DB:        sourceDb,
			Throttler: throttler,

			BuildSelect:               nil,
			BatchSize:                 &config.DataIterationBatchSize,
			BatchSizePerTableOverride: config.DataIterationBatchSizePerTableOverride,
			ReadRetries:               config.DBReadRetries,
		},
		StateTracker: ghostferry.NewStateTracker(config.DataIterationConcurrency * 10),
	}

	this.receivedRows = make(map[string][]ghostferry.RowData, 0)

	this.di.AddBatchListener(func(ev *ghostferry.RowBatch) error {
		this.receivedRows[ev.TableSchema().Name] = append(this.receivedRows[ev.TableSchema().Name], ev.Values()...)
		return nil
	})
}

func (this *DataIteratorTestSuite) TestNoEventsForEmptyTable() {
	_, err := this.Ferry.SourceDB.Query(fmt.Sprintf("DELETE FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestTable1Name))
	_, err = this.Ferry.SourceDB.Query(fmt.Sprintf("DELETE FROM `%s`.`%s`", testhelpers.TestSchemaName, testhelpers.TestCompressedTable1Name))
	this.Require().Nil(err)

	this.di.Run(this.tables)

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

	this.di.Run(this.tables)

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

	this.di.Run(this.tables)

	this.Require().True(wasNotified)
}

func (this *DataIteratorTestSuite) completedTables() map[string]bool {
	return this.di.StateTracker.Serialize(nil, nil).CompletedTables
}

func (this *DataIteratorTestSuite) TestDataIterationBatchSizePerTableOverride() {
	this.Ferry.DataIterationBatchSizePerTableOverride = &ghostferry.DataIterationBatchSizePerTableOverride{
		MinRowSize: 1000,
		MaxRowSize: 10000,
		ControlPoints: map[int]uint64{
			1000:  5000,
			10000: 200,
			3000:  4000,
			4000:  3500,
			5000:  3000,
		},
		TableOverride: map[string]map[string]uint64{},
	}
	err := this.Ferry.DataIterationBatchSizePerTableOverride.UpdateBatchSizes(this.Ferry.SourceDB, this.Ferry.Tables)
	this.di.CursorConfig.BatchSizePerTableOverride = this.Ferry.DataIterationBatchSizePerTableOverride
	this.Require().Nil(err)

	for _, table := range this.tables {
		// AVG_ROW_LENGTH for both tables are 3276
		// Using linear interpolation with points ControlPoints[3000] and ControlPoints[4000] gives 3862
		this.Require().Equal(uint64(3862), this.di.CursorConfig.GetBatchSize(table.Schema, table.Name))
	}
	this.Require().Equal(this.Ferry.Config.DataIterationBatchSize, this.di.CursorConfig.GetBatchSize("DNE", "DNE"))
}

func (this *DataIteratorTestSuite) TestDataIterationBatchSizePerTableOverrideMinRowSize() {
	this.Ferry.DataIterationBatchSizePerTableOverride = &ghostferry.DataIterationBatchSizePerTableOverride{
		MinRowSize: 5000,
		MaxRowSize: 10000,
		ControlPoints: map[int]uint64{
			10000: 200,
			5000:  3000,
		},
		TableOverride: map[string]map[string]uint64{},
	}
	err := this.Ferry.DataIterationBatchSizePerTableOverride.UpdateBatchSizes(this.Ferry.SourceDB, this.Ferry.Tables)
	this.di.CursorConfig.BatchSizePerTableOverride = this.Ferry.DataIterationBatchSizePerTableOverride
	this.Require().Nil(err)

	for _, table := range this.tables {
		// AVG_ROW_LENGTH for both tables are 3276
		// since 3276 < MinRowSize, we default to use point ControlPoints[5000]
		this.Require().Equal(uint64(3000), this.di.CursorConfig.GetBatchSize(table.Schema, table.Name))
	}
}

func (this *DataIteratorTestSuite) TestDataIterationBatchSizePerTableOverrideMaxRowSize() {
	this.Ferry.DataIterationBatchSizePerTableOverride = &ghostferry.DataIterationBatchSizePerTableOverride{
		MinRowSize: 1000,
		MaxRowSize: 3000,
		ControlPoints: map[int]uint64{
			1000: 5000,
			3000: 4000,
		},
		TableOverride: map[string]map[string]uint64{},
	}
	err := this.Ferry.DataIterationBatchSizePerTableOverride.UpdateBatchSizes(this.Ferry.SourceDB, this.Ferry.Tables)
	this.di.CursorConfig.BatchSizePerTableOverride = this.Ferry.DataIterationBatchSizePerTableOverride
	this.Require().Nil(err)

	for _, table := range this.tables {
		// AVG_ROW_LENGTH for both tables are 3276
		// since 3276 > MaxRowSize  we default to use point ControlPoints[3000]
		this.Require().Equal(uint64(4000), this.di.CursorConfig.GetBatchSize(table.Schema, table.Name))
	}
}

func (this *DataIteratorTestSuite) TestBatchSizeUpdate() {
	this.Ferry.Config.Update(ghostferry.UpdatableConfigs{DataIterationBatchSize: 1234})
	this.Require().Equal(uint64(1234), this.di.CursorConfig.GetBatchSize(testhelpers.TestSchemaName, "any_table"))
}

func (this *DataIteratorTestSuite) TestDataIterationBatchSizePerTableOverrideCalculateBatchSize() {
	this.Ferry.DataIterationBatchSizePerTableOverride = &ghostferry.DataIterationBatchSizePerTableOverride{
		MinRowSize: 100,
		MaxRowSize: 10000,
		ControlPoints: map[int]uint64{
			100:   2000,
			10000: 200,
		},
		TableOverride: map[string]map[string]uint64{},
	}

	expectedResults := map[int]int{
		1:     2000,
		100:   2000,
		200:   1981,
		500:   1927,
		1000:  1836,
		2000:  1654,
		3500:  1381,
		5000:  1108,
		10000: 200,
		15000: 200,
		20000: 200,
	}

	for rowSize, expectedBatchSize := range expectedResults {
		batchSize := this.Ferry.DataIterationBatchSizePerTableOverride.CalculateBatchSize(rowSize)
		this.Require().Equal(batchSize, expectedBatchSize)
	}
}

func TestDataIterator(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &DataIteratorTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
