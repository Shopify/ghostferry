package test

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/go-mysql-org/go-mysql/schema"
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

func (this *DataIteratorTestSuite) TestPaginationKeyBoundaries() {
	signed := func(values ...int64) []ghostferry.PaginationKey {
		keys := make([]ghostferry.PaginationKey, len(values))
		for i, value := range values {
			keys[i] = ghostferry.NewInt64Key(value)
		}
		return keys
	}
	cases := []struct {
		name       string
		columnType string
		keys       []ghostferry.PaginationKey
		checkpoint ghostferry.PaginationKey
		expected   []string
	}{
		{name: "all_negative", columnType: "BIGINT", keys: signed(-9, -5, -1), expected: []string{"-9", "-5", "-1"}},
		{name: "crossing_zero", columnType: "BIGINT", keys: signed(-2, -1, 0, 1, 2), expected: []string{"-2", "-1", "0", "1", "2"}},
		{name: "min_int64", columnType: "BIGINT", keys: signed(math.MinInt64, math.MinInt64+1, -1), expected: []string{"-9223372036854775808", "-9223372036854775807", "-1"}},
		{name: "max_int64", columnType: "BIGINT", keys: signed(math.MaxInt64-1, math.MaxInt64), expected: []string{"9223372036854775806", "9223372036854775807"}},
		{name: "signed_zero_only", columnType: "BIGINT", keys: signed(0), expected: []string{"0"}},
		{name: "unsigned_zero_only", columnType: "BIGINT UNSIGNED", keys: []ghostferry.PaginationKey{ghostferry.NewUint64Key(0)}, expected: []string{"0"}},
		{name: "empty_binary_only", columnType: "VARBINARY(16)", keys: []ghostferry.PaginationKey{ghostferry.NewBinaryKey([]byte{})}, expected: []string{""}},
		{name: "empty_binary_first", columnType: "VARBINARY(16)", keys: []ghostferry.PaginationKey{ghostferry.NewBinaryKey([]byte{}), ghostferry.NewBinaryKey([]byte{0}), ghostferry.NewBinaryKey([]byte{1})}, expected: []string{"", "00", "01"}},
		{name: "max_uint64", columnType: "BIGINT UNSIGNED", keys: []ghostferry.PaginationKey{ghostferry.NewUint64Key(math.MaxUint64 - 1), ghostferry.NewUint64Key(math.MaxUint64)}, expected: []string{"18446744073709551614", "18446744073709551615"}},
		{name: "resume_negative", columnType: "BIGINT", keys: signed(-9, -5, -1), checkpoint: ghostferry.NewInt64Key(-5), expected: []string{"-1"}},
		{name: "resume_zero", columnType: "BIGINT", keys: signed(-1, 0, 1), checkpoint: ghostferry.NewInt64Key(0), expected: []string{"1"}},
		{name: "resume_min_int64", columnType: "BIGINT", keys: signed(math.MinInt64, math.MinInt64+1), checkpoint: ghostferry.NewInt64Key(math.MinInt64), expected: []string{"-9223372036854775807"}},
		{name: "resume_signed_max", columnType: "BIGINT", keys: signed(math.MaxInt64), checkpoint: ghostferry.NewInt64Key(math.MaxInt64)},
		{name: "resume_unsigned_max", columnType: "BIGINT UNSIGNED", keys: []ghostferry.PaginationKey{ghostferry.NewUint64Key(math.MaxUint64)}, checkpoint: ghostferry.NewUint64Key(math.MaxUint64)},
		{name: "resume_binary_max", columnType: "VARBINARY(16)", keys: []ghostferry.PaginationKey{ghostferry.NewBinaryKey([]byte{0xff})}, checkpoint: ghostferry.NewBinaryKey([]byte{0xff})},
		{name: "resume_legacy_unsigned_on_signed", columnType: "BIGINT", keys: signed(-1, 0, 1, 2), checkpoint: ghostferry.NewUint64Key(1), expected: []string{"2"}},
		{name: "resume_legacy_unsigned_at_signed_max", columnType: "BIGINT", keys: signed(math.MaxInt64), checkpoint: ghostferry.NewUint64Key(math.MaxInt64)},
	}
	for _, tc := range cases {
		this.Run(tc.name, func() {
			table := this.createPaginationBoundaryTable(tc.columnType, tc.keys)
			this.receivedRows = make(map[string][]ghostferry.RowData)
			this.di.StateTracker = ghostferry.NewStateTracker(10)
			this.di.TargetPaginationKeys = &sync.Map{}
			if tc.checkpoint != nil {
				this.di.StateTracker.UpdateLastSuccessfulPaginationKey(table.String(), tc.checkpoint, ghostferry.RowStats{})
			}

			// A single row per batch exercises the exclusive bound at every value.
			batchSize := uint64(1)
			this.di.CursorConfig.BatchSize = &batchSize
			this.di.Run([]*ghostferry.TableSchema{table})

			var actual []string
			for _, row := range this.receivedRows[table.Name] {
				key, err := ghostferry.NewPaginationKeyFromRow(row, 0, table.GetPaginationColumn())
				this.Require().NoError(err)
				actual = append(actual, key.String())
			}
			this.Equal(tc.expected, actual)
			this.True(this.di.StateTracker.IsTableComplete(table.String()))
			maxKey, found := this.di.TargetPaginationKeys.Load(table.String())
			this.Require().True(found)
			this.Equal(0, maxKey.(ghostferry.PaginationKey).Compare(tc.keys[len(tc.keys)-1]))
		})
	}
}

func (this *DataIteratorTestSuite) createPaginationBoundaryTable(columnType string, keys []ghostferry.PaginationKey) *ghostferry.TableSchema {
	quotedTable := ghostferry.QuotedTableNameFromString(testhelpers.TestSchemaName, testhelpers.TestTable1Name)
	_, err := this.Ferry.SourceDB.Exec("DROP TABLE " + quotedTable)
	this.Require().NoError(err)
	_, err = this.Ferry.SourceDB.Exec("CREATE TABLE " + quotedTable + " (id " + columnType + " NOT NULL PRIMARY KEY)")
	this.Require().NoError(err)
	for _, key := range keys {
		_, err = this.Ferry.SourceDB.Exec("INSERT INTO "+quotedTable+" (id) VALUES (?)", key.SQLValue())
		this.Require().NoError(err)
	}
	tables, err := ghostferry.LoadTables(this.Ferry.SourceDB, &testhelpers.TestTableFilter{
		DbsFunc: testhelpers.DbApplicabilityFilter([]string{testhelpers.TestSchemaName}),
	}, nil, nil, nil, nil)
	this.Require().NoError(err)
	table := tables.Get(testhelpers.TestSchemaName, testhelpers.TestTable1Name)
	this.Require().NotNil(table)
	return table
}

func TestDefaultBuildSelectCheckpointBounds(t *testing.T) {
	column := schema.TableColumn{Name: "id", Type: schema.TYPE_NUMBER}
	table := &ghostferry.TableSchema{
		Table:               &schema.Table{Schema: "test", Name: "rows", Columns: []schema.TableColumn{column}},
		PaginationKeyColumn: &column,
	}
	cases := []struct {
		name string
		key  ghostferry.PaginationKey
		args []interface{}
	}{
		{name: "initial"},
		{name: "negative", key: ghostferry.NewInt64Key(-1), args: []interface{}{int64(-1)}},
		{name: "zero", key: ghostferry.NewInt64Key(0), args: []interface{}{int64(0)}},
		{name: "empty_binary", key: ghostferry.NewBinaryKey([]byte{}), args: []interface{}{[]byte{}}},
		{name: "max_unsigned", key: ghostferry.NewUint64Key(math.MaxUint64), args: []interface{}{uint64(math.MaxUint64)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			query, args, err := ghostferry.DefaultBuildSelect([]string{"*"}, table, tc.key, 2).ToSql()
			require.NoError(t, err)
			expected := "SELECT * FROM `test`.`rows`"
			if tc.key != nil {
				expected += " WHERE `id` > ?"
			}
			expected += " ORDER BY `id` LIMIT 2"
			require.Equal(t, expected, query)
			require.Equal(t, tc.args, args)
		})
	}
}

func TestCursorNilCheckpointStartsFetching(t *testing.T) {
	column := schema.TableColumn{Name: "id", Type: schema.TYPE_NUMBER}
	table := &ghostferry.TableSchema{
		Table:               &schema.Table{Schema: "test", Name: "rows", Columns: []schema.TableColumn{column}},
		PaginationKeyColumn: &column,
	}
	cases := []struct {
		name       string
		start, max ghostferry.PaginationKey
	}{
		{name: "negative_max", max: ghostferry.NewInt64Key(-1)},
		{name: "zero_max", max: ghostferry.NewInt64Key(0)},
		{name: "empty_binary_max", max: ghostferry.NewBinaryKey([]byte{})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batchSize := uint64(1)
			stop := fmt.Errorf("stop before querying database")
			config := &ghostferry.CursorConfig{
				BatchSize:   &batchSize,
				ReadRetries: 1,
				BuildSelect: func(_ []string, _ *ghostferry.TableSchema, key ghostferry.PaginationKey, _ uint64) (squirrel.SelectBuilder, error) {
					require.Equal(t, tc.start, key)
					return squirrel.SelectBuilder{}, stop
				},
			}
			cursor := config.NewCursorWithoutRowLock(table, tc.start, tc.max)
			require.ErrorIs(t, cursor.Each(func(*ghostferry.RowBatch) error {
				t.Fatal("query builder failed, so no batch should be emitted")
				return nil
			}), stop)
		})
	}
}

func TestCursorAtMaximumCheckpointDoesNotQuery(t *testing.T) {
	column := schema.TableColumn{Name: "id", Type: schema.TYPE_NUMBER}
	table := &ghostferry.TableSchema{
		Table:               &schema.Table{Schema: "test", Name: "rows", Columns: []schema.TableColumn{column}},
		PaginationKeyColumn: &column,
	}
	config := &ghostferry.CursorConfig{}
	cases := []struct {
		name       string
		start, max ghostferry.PaginationKey
	}{
		{name: "signed", start: ghostferry.NewInt64Key(math.MaxInt64), max: ghostferry.NewInt64Key(math.MaxInt64)},
		{name: "unsigned", start: ghostferry.NewUint64Key(math.MaxUint64), max: ghostferry.NewUint64Key(math.MaxUint64)},
		{name: "legacy_unsigned_on_signed", start: ghostferry.NewUint64Key(math.MaxInt64), max: ghostferry.NewInt64Key(math.MaxInt64)},
		{name: "binary", start: ghostferry.NewBinaryKey([]byte{0xff}), max: ghostferry.NewBinaryKey([]byte{0xff})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// No database is configured: entering Fetch would panic.
			cursor := config.NewCursor(table, tc.start, tc.max)
			require.NoError(t, cursor.Each(func(*ghostferry.RowBatch) error {
				t.Fatal("completed checkpoint must not emit a batch")
				return nil
			}))
		})
	}
}

func TestDataIterator(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &DataIteratorTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
