package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DataIteratorTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	di                *ghostferry.DataIterator
	wg                *sync.WaitGroup
	tables            []*ghostferry.TableSchema
	receivedRows      map[string][]ghostferry.RowData
	receivedRowsMutex sync.Mutex
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

	config.UpdatableConfig.DataIterationBatchSize = 2

	this.di = &ghostferry.DataIterator{
		DB:          sourceDb,
		Concurrency: config.DataIterationConcurrency,

		ErrorHandler: errorHandler,
		CursorConfig: &ghostferry.CursorConfig{
			DB:        sourceDb,
			Throttler: throttler,

			BuildSelect:               nil,
			BatchSize:                 &config.UpdatableConfig.DataIterationBatchSize,
			BatchSizePerTableOverride: config.DataIterationBatchSizePerTableOverride,
			ReadRetries:               config.DBReadRetries,
		},
		StateTracker: ghostferry.NewStateTracker(config.DataIterationConcurrency * 10),
		TargetPaginationKeys: &sync.Map{},
	}

	this.receivedRows = make(map[string][]ghostferry.RowData, 0)

	this.di.AddBatchListener(func(ev *ghostferry.RowBatch) error {
		this.receivedRowsMutex.Lock()
		defer this.receivedRowsMutex.Unlock()

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
	this.Require().Equal(this.Ferry.Config.UpdatableConfig.DataIterationBatchSize, this.di.CursorConfig.GetBatchSize("DNE", "DNE"))
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
	this.Ferry.Config.Update(ghostferry.UpdatableConfig{DataIterationBatchSize: 1234})
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

// copyThrottleIteratorRun records the batches of a DataIterator copying
// gftest.t1.
type copyThrottleIteratorRun struct {
	ferry  *testhelpers.TestFerry
	di     *ghostferry.DataIterator
	tables []*ghostferry.TableSchema

	mu         sync.Mutex
	batchSizes []int
	rows       [][2]interface{}
}

func newCopyThrottleIteratorRun(t *testing.T, rows int, batchSize uint64, throttler ghostferry.Throttler, recovery ghostferry.DataIterationThrottleRecoveryConfig) *copyThrottleIteratorRun {
	ferry := testhelpers.NewTestFerry()
	require.NoError(t, ferry.Initialize())

	dropTestSchema := func() {
		for _, db := range []*sql.DB{ferry.SourceDB, ferry.TargetDB} {
			_, err := db.Exec("DROP DATABASE IF EXISTS gftest")
			require.NoError(t, err)
		}
	}
	dropTestSchema()
	t.Cleanup(dropTestSchema)
	testhelpers.SeedInitialData(ferry.SourceDB, "gftest", "t1", rows)

	tables, err := ghostferry.LoadTables(ferry.SourceDB, &testhelpers.TestTableFilter{
		DbsFunc: testhelpers.DbApplicabilityFilter([]string{"gftest"}),
	}, nil, nil, nil, nil)
	require.NoError(t, err)

	run := &copyThrottleIteratorRun{ferry: ferry, tables: tables.AsSlice()}
	run.di = &ghostferry.DataIterator{
		DB:               ferry.SourceDB,
		Concurrency:      4,
		ThrottleRecovery: recovery,
		ErrorHandler:     ferry.ErrorHandler,
		CursorConfig: &ghostferry.CursorConfig{
			DB:          ferry.SourceDB,
			Throttler:   throttler,
			BatchSize:   &batchSize,
			ReadRetries: 1,
		},
		StateTracker:         ghostferry.NewStateTracker(10),
		TargetPaginationKeys: &sync.Map{},
	}
	run.di.AddBatchListener(func(batch *ghostferry.RowBatch) error {
		run.mu.Lock()
		defer run.mu.Unlock()
		run.batchSizes = append(run.batchSizes, batch.Size())
		for _, row := range batch.Values() {
			run.rows = append(run.rows, [2]interface{}{row[0], string(row[1].([]byte))})
		}
		return nil
	})
	return run
}

func (r *copyThrottleIteratorRun) start() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.di.Run(r.tables)
	}()
	return done
}

func (r *copyThrottleIteratorRun) copiedBatchSizes() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.batchSizes...)
}

// requireSourceRowsCopied asserts the listener received every source row
// once, in order.
func (r *copyThrottleIteratorRun) requireSourceRowsCopied(t *testing.T) {
	t.Helper()
	rows, err := r.ferry.SourceDB.Query("SELECT id, data FROM gftest.t1 ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var expected [][2]interface{}
	for rows.Next() {
		var id int64
		var data string
		require.NoError(t, rows.Scan(&id, &data))
		expected = append(expected, [2]interface{}{id, data})
	}
	require.NoError(t, rows.Err())

	r.mu.Lock()
	defer r.mu.Unlock()
	require.Equal(t, expected, r.rows)
}

func waitForIterator(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("data iterator did not finish")
	}
}

func TestCopyThrottleDisabledRecoveryKeepsHardThrottle(t *testing.T) {
	throttler := &ghostferry.PauserThrottler{}
	throttler.SetPaused(true)
	run := newCopyThrottleIteratorRun(t, 1000, 200, throttler, ghostferry.DataIterationThrottleRecoveryConfig{Disabled: true})

	done := run.start()
	time.Sleep(time.Second)
	require.Empty(t, run.copiedBatchSizes(), "copied while paused")

	throttler.SetPaused(false)
	waitForIterator(t, done)
	require.Equal(t, []int{200, 200, 200, 200, 200}, run.copiedBatchSizes())
	run.requireSourceRowsCopied(t)
}

func TestCopyThrottleResumesSingleTableWithOneRowBatches(t *testing.T) {
	throttler := &ghostferry.PauserThrottler{}
	throttler.SetPaused(true)
	run := newCopyThrottleIteratorRun(t, 5, 7, throttler, ghostferry.DataIterationThrottleRecoveryConfig{RecoveryInterval: "1h"})

	done := run.start()
	time.Sleep(time.Second)
	require.Empty(t, run.copiedBatchSizes(), "copied while paused")

	// One eighth of 7 rows still copies one row per batch.
	throttler.SetPaused(false)
	waitForIterator(t, done)
	require.Equal(t, []int{1, 1, 1, 1, 1}, run.copiedBatchSizes())
	run.requireSourceRowsCopied(t)
}

// RunStandaloneDataCopy runs during cutover with the throttler disabled and
// without Ferry.Run's supporting services. A paused throttler must not block it.
func TestCopyThrottleStandaloneCopyBypassesPause(t *testing.T) {
	tables := []string{"t1", "t2"}
	ferry := testhelpers.NewTestFerry()

	source, err := ferry.Source.SqlDB(nil)
	require.NoError(t, err)
	defer source.Close()
	target, err := ferry.Target.SqlDB(nil)
	require.NoError(t, err)
	defer target.Close()

	dropTestSchema := func() {
		for _, db := range []*sql.DB{source, target} {
			_, err := db.Exec("DROP DATABASE IF EXISTS gftest")
			require.NoError(t, err)
		}
	}
	dropTestSchema()
	defer dropTestSchema()
	for _, table := range tables {
		testhelpers.SeedInitialData(source, "gftest", table, 500)
		testhelpers.SeedInitialData(target, "gftest", table, 0)
	}

	throttler := &ghostferry.PauserThrottler{}
	throttler.SetPaused(true)
	throttler.SetDisabled(true)
	ferry.Throttler = throttler
	require.NoError(t, ferry.Initialize())

	done := make(chan error, 1)
	go func() {
		done <- ferry.RunStandaloneDataCopy(ferry.Tables.AsSlice())
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("standalone data copy did not finish while the throttler was paused and disabled")
	}

	for _, table := range tables {
		rows := testhelpers.AssertQueriesHaveEqualResult(t, ferry.Ferry, fmt.Sprintf("SELECT * FROM gftest.%s ORDER BY id", table))
		require.Len(t, rows, 500)
	}
}
