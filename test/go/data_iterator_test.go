package test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
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
		StateTracker:         ghostferry.NewStateTracker(config.DataIterationConcurrency * 10),
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

func (this *DataIteratorTestSuite) TestCompositeKeyIterationOrder() {
	// Create a table with a composite primary key (tenant_id, id)
	// We use this structure to test that iteration respects the lexicographical order
	// i.e., (1, 100) < (2, 1)
	dbName := testhelpers.TestSchemaName
	tableName := "composite_key_test"

	// Drop if exists
	_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", dbName, tableName))
	this.Require().Nil(err)

	// Create table
	query := fmt.Sprintf("CREATE TABLE %s.%s (tenant_id int, id int, data varchar(255), PRIMARY KEY (tenant_id, id))", dbName, tableName)
	_, err = this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	// Insert data in a scrambled order to ensure the DB returns them sorted by PK during iteration
	values := []struct {
		tenant_id int
		id        int
		data      string
	}{
		{2, 1, "t2-id1"},
		{1, 10, "t1-id10"},
		{1, 1, "t1-id1"},
		{3, 1, "t3-id1"},
		{1, 2, "t1-id2"},
		{2, 5, "t2-id5"},
	}

	for _, v := range values {
		_, err = this.Ferry.SourceDB.Exec(fmt.Sprintf("INSERT INTO %s.%s (tenant_id, id, data) VALUES (?, ?, ?)", dbName, tableName), v.tenant_id, v.id, v.data)
		this.Require().Nil(err)
	}

	// Configure Ghostferry to use this table
	filter := &testhelpers.TestTableFilter{
		DbsFunc:    testhelpers.DbApplicabilityFilter([]string{dbName}),
		TablesFunc: nil,
	}

	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, filter, nil, nil, nil, nil)
	this.Require().Nil(err)

	targetTable := tables.Get(dbName, tableName)
	this.Require().NotNil(targetTable)
	this.Require().Equal(2, len(targetTable.GetPaginationColumns()))

	// Setup DataIterator with a batch size of 2 to force multiple batches and pagination logic
	batchSize := uint64(2)
	dataIterator := &ghostferry.DataIterator{
		DB:           this.Ferry.SourceDB,
		Concurrency:  1,
		ErrorHandler: this.Ferry.ErrorHandler,
		CursorConfig: &ghostferry.CursorConfig{
			DB:        this.Ferry.SourceDB,
			BatchSize: &batchSize,
		},
		StateTracker:         ghostferry.NewStateTracker(10),
		TargetPaginationKeys: &sync.Map{},
	}

	// Collect rows
	var collectedRows []struct{ t, i int }
	dataIterator.AddBatchListener(func(batch *ghostferry.RowBatch) error {
		vals := batch.Values()
		for _, row := range vals {
			tID, _ := row.GetUint64(0)
			id, _ := row.GetUint64(1)
			collectedRows = append(collectedRows, struct{ t, i int }{int(tID), int(id)})
		}
		return nil
	})

	// Run iterator
	dataIterator.Run([]*ghostferry.TableSchema{targetTable})

	// Verify order: (1,1), (1,2), (1,10), (2,1), (2,5), (3,1)
	expected := []struct{ t, i int }{
		{1, 1},
		{1, 2},
		{1, 10},
		{2, 1},
		{2, 5},
		{3, 1},
	}

	this.Require().Equal(len(expected), len(collectedRows))
	for i := range expected {
		this.Require().Equal(expected[i], collectedRows[i], "Mismatch at index %d", i)
	}
}

func (this *DataIteratorTestSuite) TestCompositeKeyWithBinaryType() {
	// Test mixing types: (tenant_id int, uuid varbinary(16))
	dbName := testhelpers.TestSchemaName
	tableName := "composite_binary_test"

	_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", dbName, tableName))
	this.Require().Nil(err)

	query := fmt.Sprintf("CREATE TABLE %s.%s (tenant_id int, uuid varbinary(16), data varchar(255), PRIMARY KEY (tenant_id, uuid))", dbName, tableName)
	_, err = this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	// Insert data: (1, 'A'), (1, 'B'), (2, 'A')
	_, err = this.Ferry.SourceDB.Exec(fmt.Sprintf("INSERT INTO %s.%s VALUES (1, 'A', 'd1'), (2, 'A', 'd2'), (1, 'B', 'd3')", dbName, tableName))
	this.Require().Nil(err)

	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB,
		&testhelpers.TestTableFilter{DbsFunc: testhelpers.DbApplicabilityFilter([]string{dbName})},
		nil, nil, nil, nil)
	this.Require().Nil(err)
	targetTable := tables.Get(dbName, tableName)

	batchSize := uint64(1) // Force batching per row
	dataIterator := &ghostferry.DataIterator{
		DB:           this.Ferry.SourceDB,
		Concurrency:  1,
		ErrorHandler: this.Ferry.ErrorHandler,
		CursorConfig: &ghostferry.CursorConfig{
			DB:        this.Ferry.SourceDB,
			BatchSize: &batchSize,
		},
		StateTracker:         ghostferry.NewStateTracker(10),
		TargetPaginationKeys: &sync.Map{},
	}

	var collectedRows []string
	dataIterator.AddBatchListener(func(batch *ghostferry.RowBatch) error {
		for _, row := range batch.Values() {
			tID, _ := row.GetUint64(0)
			uuid := row[1].([]byte)
			collectedRows = append(collectedRows, fmt.Sprintf("%d-%s", tID, string(uuid)))
		}
		return nil
	})

	dataIterator.Run([]*ghostferry.TableSchema{targetTable})

	expected := []string{"1-A", "1-B", "2-A"}
	this.Require().Equal(expected, collectedRows)
}

func (this *DataIteratorTestSuite) TestThreeColumnCompositeKeyIterationOrder() {
	// Test iteration with a 3-column composite primary key (region_id, tenant_id, id)
	dbName := testhelpers.TestSchemaName
	tableName := "three_col_composite_test"

	// Drop if exists
	_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", dbName, tableName))
	this.Require().Nil(err)

	// Create table
	query := fmt.Sprintf("CREATE TABLE %s.%s (region_id int, tenant_id int, id int, data varchar(255), PRIMARY KEY (region_id, tenant_id, id))", dbName, tableName)
	_, err = this.Ferry.SourceDB.Exec(query)
	this.Require().Nil(err)

	// Insert data in scrambled order to test sorting
	values := []struct {
		region_id int
		tenant_id int
		id        int
		data      string
	}{
		{2, 1, 1, "r2-t1-id1"},
		{1, 2, 3, "r1-t2-id3"},
		{1, 1, 1, "r1-t1-id1"},
		{1, 1, 2, "r1-t1-id2"},
		{1, 2, 1, "r1-t2-id1"},
		{2, 1, 2, "r2-t1-id2"},
		{1, 1, 3, "r1-t1-id3"},
		{2, 2, 1, "r2-t2-id1"},
	}

	for _, v := range values {
		_, err = this.Ferry.SourceDB.Exec(fmt.Sprintf("INSERT INTO %s.%s (region_id, tenant_id, id, data) VALUES (?, ?, ?, ?)", dbName, tableName), v.region_id, v.tenant_id, v.id, v.data)
		this.Require().Nil(err)
	}

	// Configure Ghostferry
	filter := &testhelpers.TestTableFilter{
		DbsFunc:    testhelpers.DbApplicabilityFilter([]string{dbName}),
		TablesFunc: nil,
	}

	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, filter, nil, nil, nil, nil)
	this.Require().Nil(err)

	targetTable := tables.Get(dbName, tableName)
	this.Require().NotNil(targetTable)
	this.Require().Equal(3, len(targetTable.GetPaginationColumns()))

	// Setup DataIterator with a batch size of 2
	batchSize := uint64(2)
	dataIterator := &ghostferry.DataIterator{
		DB:           this.Ferry.SourceDB,
		Concurrency:  1,
		ErrorHandler: this.Ferry.ErrorHandler,
		CursorConfig: &ghostferry.CursorConfig{
			DB:        this.Ferry.SourceDB,
			BatchSize: &batchSize,
		},
		StateTracker:         ghostferry.NewStateTracker(10),
		TargetPaginationKeys: &sync.Map{},
	}

	// Collect rows
	var collectedRows []struct{ r, t, i int }
	dataIterator.AddBatchListener(func(batch *ghostferry.RowBatch) error {
		vals := batch.Values()
		for _, row := range vals {
			rID, _ := row.GetUint64(0)
			tID, _ := row.GetUint64(1)
			id, _ := row.GetUint64(2)
			collectedRows = append(collectedRows, struct{ r, t, i int }{int(rID), int(tID), int(id)})
		}
		return nil
	})

	dataIterator.Run([]*ghostferry.TableSchema{targetTable})

	// Verify order: (1,1,1), (1,1,2), (1,1,3), (1,2,1), (1,2,3), (2,1,1), (2,1,2), (2,2,1)
	expected := []struct{ r, t, i int }{
		{1, 1, 1},
		{1, 1, 2},
		{1, 1, 3},
		{1, 2, 1},
		{1, 2, 3},
		{2, 1, 1},
		{2, 1, 2},
		{2, 2, 1},
	}

	this.Require().Equal(len(expected), len(collectedRows))
	for i := range expected {
		this.Require().Equal(expected[i], collectedRows[i], "Mismatch at index %d", i)
	}
}

func TestDataIterator(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &DataIteratorTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
