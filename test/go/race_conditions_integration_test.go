package test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/require"
)

func setupSingleEntryTable(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
	testhelpers.SeedInitialData(sourceDB, "gftest", "table1", 1)
	testhelpers.SeedInitialData(targetDB, "gftest", "table1", 0)
}

func annotate(queries []string, marginalia string) []string {
	annotatedQueries := make([]string, len(queries))
	for i, q := range queries {
		annotatedQueries[i] = sql.AnnotateStmt(q, marginalia)
	}
	return annotatedQueries
}

func TestSelectUpdateBinlogCopy(t *testing.T) {
	testcase := testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleEntryTable,
		Ferry:       testhelpers.NewTestFerry(),
	}

	testcase.Ferry.BeforeBatchCopyListener = func(batch *ghostferry.RowBatch) error {
		queries := make([]string, len(batch.Values()))
		for i, row := range batch.Values() {
			id := row[0].(int64)
			queries[i] = "UPDATE gftest.table1 SET data = 'changed' WHERE id = ?"

			go func(query string) {
				_, err := testcase.Ferry.SourceDB.Exec(query, id)
				if err != nil {
					panic(err)
				}
			}(queries[i])
		}

		queries = annotate(queries, testcase.Ferry.Source.Marginalia)

		// Waiting for sure until we can see the queries as they will be
		// locked due to the SELECT FOR UPDATE that is being performed.
		for !testhelpers.ProcessListContainsQueries(testcase.Ferry.SourceDB, queries) {
			time.Sleep(200 * time.Millisecond)
		}

		return nil
	}

	testcase.CustomVerifyAction = func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
		testcase.AssertOnlyDataOnSourceAndTargetIs("changed")
	}

	testcase.Run()
}

func TestUpdateBinlogSelectCopy(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleEntryTable,
		Ferry:       testhelpers.NewTestFerry(),
	}

	testcase.AfterStartBinlogStreaming = func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
		_, err := f.SourceDB.Exec("UPDATE gftest.table1 SET data = 'changed' LIMIT 1")
		testhelpers.PanicIfError(err)
	}

	testcase.CustomVerifyAction = func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
		testcase.AssertOnlyDataOnSourceAndTargetIs("changed")
	}

	testcase.Run()
}

func TestMasterChangingBeforeStoppingBinlogStreaming(t *testing.T) {
	if os.Getenv("GHOSTFERRY_BINLOG_COORDINATE_MODE") == string(ghostferry.BinlogCoordinateGTID) {
		t.Skip("master-demotion detection is mode-independent; covered by the file/position run")
	}

	ferry := testhelpers.NewTestFerry()

	sourceDB, err := ferry.Source.SqlDB(nil)
	testhelpers.PanicIfError(err)
	defer sourceDB.Close()

	errHandler := &testhelpers.ErrorHandler{}
	ferry.ErrorHandler = errHandler

	ferry.WaitUntilReplicaIsCaughtUpToMaster = &ghostferry.WaitUntilReplicaIsCaughtUpToMaster{
		MasterDB: sourceDB,
		ReplicatedMasterPositionFetcher: &ghostferry.ReplicatedMasterPositionViaCustomQuery{
			Query: "SELECT 'mysql-bin.9999999', 9999999",
		},
	}

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleEntryTable,
		Ferry:       ferry,
	}

	testcase.BeforeStoppingBinlogStreaming = func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
		_, err := sourceDB.Exec("SET GLOBAL read_only = ON")
		testhelpers.PanicIfError(err)
	}

	testcase.Run()

	require.NotNil(t, errHandler.LastError)
	require.Equal(t, "source master is no longer a master writer", errHandler.LastError.Error())
}

func TestSelectCopyUpdateBinlog(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleEntryTable,
		Ferry:       testhelpers.NewTestFerry(),
	}

	testcase.BeforeStoppingBinlogStreaming = func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
		_, err := f.SourceDB.Exec("UPDATE gftest.table1 SET data = 'changed' LIMIT 1")
		testhelpers.PanicIfError(err)
	}

	testcase.CustomVerifyAction = func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
		testcase.AssertOnlyDataOnSourceAndTargetIs("changed")
	}

	testcase.Run()
}

func TestOnlyDeleteRowWithMaxPaginationKey(t *testing.T) {
	testcase := &testhelpers.IntegrationTestCase{
		T: t,
		SetupAction: func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			testhelpers.SeedInitialData(sourceDB, "gftest", "table1", 2)
			testhelpers.SeedInitialData(targetDB, "gftest", "table1", 0)
		},
		Ferry: testhelpers.NewTestFerry(),
	}

	testcase.Ferry.UpdatableConfig.DataIterationBatchSize = 1

	lastRowDeleted := false
	testcase.Ferry.BeforeBatchCopyListener = func(batch *ghostferry.RowBatch) error {
		if lastRowDeleted {
			return nil
		}

		r, err := testcase.Ferry.SourceDB.Exec("DELETE FROM gftest.table1 ORDER BY id DESC LIMIT 1")
		if err != nil {
			return err
		}

		rowsAffected, err := r.RowsAffected()
		if err != nil {
			return err
		}

		if rowsAffected != 1 {
			return fmt.Errorf("there should be 1 row deleted, but %d rows were deleted", rowsAffected)
		}

		lastRowDeleted = true

		return nil
	}

	testcase.Run()
}

type heldCopyBatch struct {
	table   string
	rows    int
	firstID int64
	release chan struct{}
}

// copyBatchGate holds every copy batch in its listener until the test
// releases it, or until the gate is opened.
type copyBatchGate struct {
	batches chan heldCopyBatch
	opened  chan struct{}
	once    sync.Once
}

func newCopyBatchGate() *copyBatchGate {
	return &copyBatchGate{
		batches: make(chan heldCopyBatch, 64),
		opened:  make(chan struct{}),
	}
}

func (g *copyBatchGate) listener(batch *ghostferry.RowBatch) error {
	b := heldCopyBatch{
		table:   batch.TableSchema().Name,
		rows:    batch.Size(),
		firstID: batch.Values()[0][0].(int64),
		release: make(chan struct{}),
	}

	select {
	case g.batches <- b:
	case <-g.opened:
		return nil
	}

	select {
	case <-b.release:
	case <-g.opened:
	}
	return nil
}

func (g *copyBatchGate) next(t *testing.T) heldCopyBatch {
	t.Helper()
	select {
	case b := <-g.batches:
		return b
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a copy batch")
		return heldCopyBatch{}
	}
}

func (g *copyBatchGate) requireNoBatch(t *testing.T) {
	t.Helper()
	require.Empty(t, g.batches, "unexpected copy batch")
}

// open lets held and future batches proceed.
func (g *copyBatchGate) open() {
	g.once.Do(func() { close(g.opened) })
}

func waitUntil(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", description)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func countRowsWithID(t *testing.T, db *sql.DB, table string, id int) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM gftest.%s WHERE id = ?", table), id).Scan(&count))
	return count
}

// execWithLockWaitTimeout runs a locking statement that fails after one second
// if another transaction holds a conflicting row lock.
func execWithLockWaitTimeout(t *testing.T, db *sql.DB, query string, args ...interface{}) error {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = tx.Exec("SET SESSION innodb_lock_wait_timeout = 1")
	require.NoError(t, err)

	if _, err = tx.Exec(query, args...); err != nil {
		return err
	}
	return tx.Commit()
}

func TestCopyThrottleResumesOneSmallBatchWithoutSourceLocks(t *testing.T) {
	const rowsPerTable = 1000
	tables := []string{"t1", "t2", "t3", "t4"}
	gate := newCopyBatchGate()

	testcase := &testhelpers.IntegrationTestCase{
		T: t,
		SetupAction: func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			for _, table := range tables {
				testhelpers.SeedInitialData(sourceDB, "gftest", table, rowsPerTable)
				testhelpers.SeedInitialData(targetDB, "gftest", table, 0)
			}
		},
		AfterStartBinlogStreaming: func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			f.DataIterator.CursorConfig.BatchSizePerTableOverride = &ghostferry.DataIterationBatchSizePerTableOverride{
				TableOverride: map[string]map[string]uint64{"gftest": {"t4": 80}},
			}
		},
		Ferry: testhelpers.NewTestFerry(),
	}
	defer testcase.Teardown()
	defer gate.open()

	ferry := testcase.Ferry
	ferry.DataIterationConcurrency = 4
	ferry.UpdatableConfig.DataIterationBatchSize = 200
	ferry.DataIterationThrottleRecovery.RecoveryInterval = "1h"
	ferry.BeforeBatchCopyListener = gate.listener

	testcase.Setup()
	testcase.StartFerryAndDataWriter()

	baseRows := map[string]int{"t1": 200, "t2": 200, "t3": 200, "t4": 80}
	running := map[string]heldCopyBatch{}
	for range tables {
		b := gate.next(t)
		require.Equal(t, baseRows[b.table], b.rows, "first batch of %s", b.table)
		running[b.table] = b
	}
	require.Len(t, running, len(tables))

	// Pause while every table is being copied and let the running batches
	// finish. Their next batches wait for admission.
	ferry.Throttler.SetPaused(true)
	for _, b := range running {
		close(b.release)
	}
	waitUntil(t, "the running copy batches released their source locks", func() bool {
		for table, b := range running {
			err := execWithLockWaitTimeout(t, testcase.SourceDB, fmt.Sprintf("SELECT id FROM gftest.%s WHERE id <= ? FOR UPDATE", table), b.rows)
			if err != nil {
				require.ErrorContains(t, err, "Lock wait timeout exceeded")
				return false
			}
		}
		return true
	})
	gate.requireNoBatch(t)

	ferry.Throttler.SetPaused(false)
	held := gate.next(t)
	require.Equal(t, baseRows[held.table]/8, held.rows, "first recovery batch of %s", held.table)

	// The admitted batch holds its source row locks while it is copied...
	err := execWithLockWaitTimeout(t, testcase.SourceDB, fmt.Sprintf("UPDATE gftest.%s SET data = 'locked' WHERE id = ?", held.table), held.firstID)
	require.ErrorContains(t, err, "Lock wait timeout exceeded")

	// ...while the batches waiting for admission have not locked any rows.
	for _, table := range tables {
		if table != held.table {
			require.NoError(t, execWithLockWaitTimeout(t, testcase.SourceDB, fmt.Sprintf("UPDATE gftest.%s SET data = 'updated while waiting'", table)))
		}
	}

	// Binlog replay does not wait for the copy recovery.
	_, err = testcase.SourceDB.Exec("INSERT INTO gftest.t1 (id, data) VALUES (?, 'sentinel')", rowsPerTable+1)
	require.NoError(t, err)
	waitUntil(t, "the sentinel row was replayed", func() bool {
		return countRowsWithID(t, testcase.TargetDB, "t1", rowsPerTable+1) == 1
	})
	gate.requireNoBatch(t)

	// Live batch size updates are scaled during recovery, as are overrides.
	ferry.Config.Update(ghostferry.UpdatableConfig{DataIterationBatchSize: 104})
	close(held.release)

	recoveryRows := map[string]int{"t1": 13, "t2": 13, "t3": 13, "t4": 10}
	seen := map[string]bool{}
	for i := 0; len(seen) < len(tables); i++ {
		require.Less(t, i, 2*len(tables), "every table gets a batch in FIFO order")
		b := gate.next(t)
		require.Equal(t, recoveryRows[b.table], b.rows, "recovery batch of %s", b.table)
		gate.requireNoBatch(t)
		seen[b.table] = true
		close(b.release)
	}

	gate.open()
	testcase.WaitUntilRowCopyIsComplete()
	testcase.SetReadonlyOnSourceDbAndStopDataWriter()
	testcase.StopStreamingAndWaitForGhostferryFinish()
	testcase.VerifyData()

	for _, table := range tables {
		testcase.AssertQueriesHaveEqualResult(fmt.Sprintf("SELECT * FROM gftest.%s ORDER BY id", table))
	}
	require.Equal(t, 1, countRowsWithID(t, testcase.TargetDB, "t1", rowsPerTable+1))
}

var copyRecoveryBatchSizes = []int{25, 50, 100, 200}

// requireRecoveryStep asserts that a batch size stays at, or moves to the
// next, recovery stage.
func requireRecoveryStep(t *testing.T, previous, next int) {
	t.Helper()
	for i, rows := range copyRecoveryBatchSizes {
		if rows == previous {
			require.True(t, next == previous || (i+1 < len(copyRecoveryBatchSizes) && next == copyRecoveryBatchSizes[i+1]),
				"batch size went from %d to %d", previous, next)
			return
		}
	}
	t.Fatalf("unexpected recovery batch size %d", previous)
}

func TestCopyThrottleLagThrottlerRecoversCopyGradually(t *testing.T) {
	const rows = 2000
	const recoveryInterval = 300 * time.Millisecond

	throttler, err := ghostferry.NewLagThrottler(&ghostferry.LagThrottlerConfig{
		Connection:     testhelpers.NewTestConfig().Target,
		MaxLag:         6,
		Query:          "SELECT MAX(throttler_lag) FROM meta.lag_table",
		UpdateInterval: "20ms",
	})
	require.NoError(t, err)
	setupLagTable(throttler.DB, context.Background())
	setLag(throttler, 1, 0)

	gate := newCopyBatchGate()
	testcase := &testhelpers.IntegrationTestCase{
		T: t,
		SetupAction: func(f *testhelpers.TestFerry, sourceDB, targetDB *sql.DB) {
			testhelpers.SeedInitialData(sourceDB, "gftest", "t1", rows)
			testhelpers.SeedInitialData(targetDB, "gftest", "t1", 0)
		},
		Ferry: testhelpers.NewTestFerry(),
	}
	defer testcase.Teardown()
	defer gate.open()

	ferry := testcase.Ferry
	ferry.Throttler = throttler
	ferry.DataIterationConcurrency = 1
	ferry.UpdatableConfig.DataIterationBatchSize = 200
	ferry.DataIterationThrottleRecovery.RecoveryInterval = recoveryInterval.String()
	ferry.BeforeBatchCopyListener = gate.listener

	testcase.Setup()
	testcase.StartFerryAndDataWriter()

	b := gate.next(t)
	require.Equal(t, 200, b.rows)

	// Lag above the threshold stops new copy batches and binlog replay.
	setLag(throttler, 1, 10)
	waitUntil(t, "the lag throttler throttles", throttler.Throttled)
	_, err = testcase.SourceDB.Exec("INSERT INTO gftest.t1 (id, data) VALUES (?, 'sentinel')", rows+1)
	require.NoError(t, err)
	close(b.release)

	time.Sleep(1500 * time.Millisecond)
	gate.requireNoBatch(t)
	require.Equal(t, 0, countRowsWithID(t, testcase.TargetDB, "t1", rows+1))

	// Fresh samples below the threshold resume both, copy with small batches.
	setLag(throttler, 1, 0)
	b = gate.next(t)
	require.Equal(t, 25, b.rows)
	waitUntil(t, "the sentinel row was replayed", func() bool {
		return countRowsWithID(t, testcase.TargetDB, "t1", rows+1) == 1
	})

	// Each batch is held for a recovery interval, completing a healthy stage.
	holdAndNext := func(b heldCopyBatch) heldCopyBatch {
		time.Sleep(recoveryInterval + 100*time.Millisecond)
		close(b.release)
		next := gate.next(t)
		requireRecoveryStep(t, b.rows, next.rows)
		return next
	}

	for i := 0; b.rows < 50; i++ {
		require.Less(t, i, 10, "copy recovery did not reach 50 row batches")
		b = holdAndNext(b)
	}

	// Without lag rows the query returns NULL: no new sample, no growth.
	_, err = throttler.DB.Exec("DELETE FROM meta.lag_table")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	for range 3 {
		b = holdAndNext(b)
		require.Equal(t, 50, b.rows)
	}

	setLag(throttler, 1, 0)
	for i := 0; b.rows < 200; i++ {
		require.Less(t, i, 10, "copy recovery did not reach full batches")
		b = holdAndNext(b)
	}

	gate.open()
	testcase.WaitUntilRowCopyIsComplete()
	testcase.SetReadonlyOnSourceDbAndStopDataWriter()
	testcase.StopStreamingAndWaitForGhostferryFinish()
	testcase.VerifyData()

	testcase.AssertQueriesHaveEqualResult("SELECT * FROM gftest.t1 ORDER BY id")
	require.Equal(t, 1, countRowsWithID(t, testcase.TargetDB, "t1", rows+1))
}
