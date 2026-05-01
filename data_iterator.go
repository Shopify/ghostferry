package ghostferry

import (
	"errors"
	"fmt"
	"sync"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
)

// TransitionChecker reports whether a table's iteration should be aborted
// because the schema-change detector has marked it as in-transition or
// recopying. Decoupled as an interface so DataIterator stays unit-testable
// and to avoid an import cycle with the detector.
type TransitionChecker interface {
	IsInTransition(schemaName, tableName string) bool
}

// DDLTracer (optional) lets DataIterator emit lines into the schema-change
// detector's trace file at moments only DataIterator can observe — recopy
// goroutine spawn, recopy iteration completion. Decoupled as an interface to
// keep data_iterator.go free of the detector type.
type DDLTracer interface {
	Trace(format string, args ...interface{})
}

// errSchemaDriftDetected is the sentinel returned from cursor.Each when we
// notice column drift between the cached schema and the live SELECT result,
// or when the table has entered transition mid-iteration. The data iterator
// catches it, marks the table complete, and lets the schema-change detector
// drive the eventual recopy.
var errSchemaDriftDetected = errors.New("schema drift detected mid-iteration; aborting cleanly for recopy")

type DataIterator struct {
	DB                *sql.DB
	Concurrency       int
	SelectFingerprint bool

	ErrorHandler ErrorHandler
	CursorConfig *CursorConfig
	StateTracker *StateTracker
	TableSorter  DataIteratorSorter

	// TransitionChecker (optional) lets the iterator abort cleanly when a
	// table enters StateInTransition or StateRecopying mid-copy. Nil disables
	// the check (legacy behavior).
	TransitionChecker TransitionChecker

	// Tracer (optional) emits DDL-trace lines for events the detector cannot
	// see — recopy goroutine spawn and iteration completion.
	Tracer DDLTracer

	TargetPaginationKeys     *sync.Map
	batchListeners           []func(*RowBatch) error
	doneListeners            []func() error
	tableCompletionListeners []func(*TableSchema)
	logger                   Logger

	// iterations tracks per-table in-flight iterateTable goroutines. The
	// schema-change detector waits on the channel for a given table before
	// running its bulk DELETE, so concurrent INSERTs from an abandoned worker
	// can't survive the delete. The channel is closed when the goroutine
	// returns (any reason, including errSchemaDriftDetected) and the entry
	// is removed from the map.
	iterationsMu sync.Mutex
	iterations   map[string]chan struct{}
}

type TableMaxPaginationKey struct {
	Table            *TableSchema
	MaxPaginationKey PaginationKey
}

func (d *DataIterator) Run(tables []*TableSchema) {
	d.logger = LogWithField("tag", "data_iterator")

	// If a state tracker is not provided, then the caller doesn't care about
	// tracking state. However, some methods are still useful so we initialize
	// a minimal local instance.
	if d.StateTracker == nil {
		d.StateTracker = NewStateTracker(0)
	}

	d.logger.WithField("tablesCount", len(tables)).Info("starting data iterator run")
	tablesWithData, emptyTables, err := MaxPaginationKeys(d.DB, tables, d.logger)
	if err != nil {
		d.ErrorHandler.Fatal("data_iterator", err)
	}

	for _, table := range emptyTables {
		d.StateTracker.MarkTableAsCompleted(table.String())
	}

	for table, maxPaginationKey := range tablesWithData {
		tableName := table.String()
		if d.StateTracker.IsTableComplete(tableName) {
			// In a previous run, the table may have been completed.
			// We don't need to reiterate those tables as it has already been done.
			delete(tablesWithData, table)
		} else {
			d.TargetPaginationKeys.Store(tableName, maxPaginationKey)
		}
	}

	tablesQueue := make(chan *TableSchema)
	wg := &sync.WaitGroup{}
	wg.Add(d.Concurrency)

	for i := 0; i < d.Concurrency; i++ {
		go func() {
			defer wg.Done()

			for {
				table, ok := <-tablesQueue
				if !ok {
					break
				}
				d.iterateTable(table)
			}
		}()
	}

	sorter := MaxTableSizeSorter{DataIterator: d}
	sortedTableData, err := sorter.Sort(tablesWithData)

	if err != nil {
		d.logger.WithError(err).Error("failed to retrieve sorted tables")
		d.ErrorHandler.Fatal("data_iterator", err)
		return
	}

	loggingIncrement := len(tablesWithData) / 50
	if loggingIncrement == 0 {
		loggingIncrement = 1
	}

	i := 0
	for _, tableData := range sortedTableData {
		tablesQueue <- tableData.Table
		i++
		if i%loggingIncrement == 0 {
			d.logger.WithField("table", tableData.Table.String()).Infof("queued table for processing (%d/%d)", i, len(sortedTableData))
		}
	}

	d.logger.Info("done queueing tables to be iterated, closing table channel")
	close(tablesQueue)

	wg.Wait()
	for _, listener := range d.doneListeners {
		listener()
	}
}

func (d *DataIterator) iterateTable(table *TableSchema) {
	logger := d.logger.WithField("table", table.String())

	// Register an iteration-in-flight channel so the detector can block on it
	// before running a bulk DELETE. Done is closed when the function returns
	// for any reason — including the errSchemaDriftDetected abort path — so
	// the detector unblocks promptly even on aborted iterations.
	d.iterationsMu.Lock()
	if d.iterations == nil {
		d.iterations = map[string]chan struct{}{}
	}
	done := make(chan struct{})
	d.iterations[table.String()] = done
	d.iterationsMu.Unlock()
	defer func() {
		d.iterationsMu.Lock()
		if cur, ok := d.iterations[table.String()]; ok && cur == done {
			delete(d.iterations, table.String())
		}
		d.iterationsMu.Unlock()
		close(done)
	}()

	targetPaginationKeyInterface, found := d.TargetPaginationKeys.Load(table.String())
	if !found {
		err := fmt.Errorf("%s not found in TargetPaginationKeys, this is likely a programmer error", table.String())
		logger.WithError(err).Error("this is definitely a bug")
		d.ErrorHandler.Fatal("data_iterator", err)
		return
	}

	startPaginationKey := d.StateTracker.LastSuccessfulPaginationKey(table.String(), table)
	if startPaginationKey.IsMax() {
		err := fmt.Errorf("%v has been marked as completed but a table iterator has been spawned, this is likely a programmer error which resulted in the inconsistent starting state", table.String())
		logger.WithError(err).Error("this is definitely a bug")
		d.ErrorHandler.Fatal("data_iterator", err)
		return
	}

	cursor := d.CursorConfig.NewCursor(table, startPaginationKey, targetPaginationKeyInterface.(PaginationKey))
	if d.SelectFingerprint {
		if len(cursor.ColumnsToSelect) == 0 {
			cursor.ColumnsToSelect = []string{"*"}
		}

		cursor.ColumnsToSelect = append(cursor.ColumnsToSelect, table.RowMd5Query())
	}

	err := cursor.Each(func(batch *RowBatch) error {
		metrics.Count("RowEvent", int64(batch.Size()), []MetricTag{
			MetricTag{"table", table.Name},
			MetricTag{"source", "table"},
		}, 1.0)

		if d.SelectFingerprint {
			fingerprints := make(map[string][]byte)
			rows := make([]RowData, batch.Size())
			paginationColumn := table.GetPaginationColumn()

			for i, rowData := range batch.Values() {
				paginationKey, err := NewPaginationKeyFromRow(rowData, batch.PaginationKeyIndex(), paginationColumn)
				if err != nil {
					logger.WithError(err).Error("failed to get paginationKey data")
					return err
				}

				fingerprints[paginationKey.String()] = rowData[len(rowData)-1].([]byte)
				rows[i] = rowData[:len(rowData)-1]
			}

			batch = &RowBatch{
				values:             rows,
				paginationKeyIndex: batch.PaginationKeyIndex(),
				table:              table,
				fingerprints:       fingerprints,
				columns:            batch.columns[:len(batch.columns)-1],
			}
		}

		// Abort iteration cleanly if the schema-change detector has flagged
		// this table mid-copy, or if the live source columns now drift from
		// the cached schema (covers the race window before the DDL binlog
		// event reaches the detector). Either way the recopy mechanism will
		// re-iterate from a refreshed schema once source/target converge.
		if d.TransitionChecker != nil && d.TransitionChecker.IsInTransition(table.Schema, table.Name) {
			logger.Info("aborting iteration: table entered transition mid-copy")
			return errSchemaDriftDetected
		}
		if columnsDriftFromCache(batch.columns, table) {
			logger.WithFields(Fields{
				"liveColumns":   batch.columns,
				"cachedColumns": ConvertTableColumnsToStrings(table.Columns),
			}).Info("aborting iteration: source column list drifted from cached schema")
			return errSchemaDriftDetected
		}

		for _, listener := range d.batchListeners {
			err := listener(batch)
			if err != nil {
				logger.WithError(err).Error("failed to process row batch with listeners")
				return err
			}
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, errSchemaDriftDetected) {
			// Drop the partial cursor so the table is visibly back in the
			// "not started" state — neither completed nor mid-copy. The
			// detector's recopy() also calls ResetTable before re-iteration
			// (via RequeueTable), but doing it here makes ferry's externally
			// observable state accurate from the instant the drift is seen.
			//
			// Do NOT mark complete and do NOT fire completion listeners.
			// The detector waits on AwaitIterationDrained (closed by the
			// deferred channel close above) to know when this goroutine
			// has exited and it is safe to DELETE.
			d.StateTracker.ResetTable(table.String())
			logger.Info("iteration aborted for schema drift; table reset to pending, awaiting detector recopy")
			return
		}
		switch e := err.(type) {
		case BatchWriterVerificationFailed:
			logger.WithField("incorrect_tables", e.table).Error(e.Error())
			d.ErrorHandler.Fatal("inline_verifier", err)
		default:
			logger.WithError(err).Error("failed to iterate table")
			d.ErrorHandler.Fatal("data_iterator", err)
		}
	}

	logger.Debug("table iteration completed")
	if d.Tracer != nil {
		d.Tracer.Trace("iterateTable_completed table=%s", table.String())
	}

	// Right now the BatchWriter.WriteRowBatch happens synchronously in
	// this method. If it ever becomes async, this MarkTableAsCompleted
	// call MUST be done in WriteRowBatch somehow.
	d.StateTracker.MarkTableAsCompleted(table.String())

	for _, listener := range d.tableCompletionListeners {
		listener(table)
	}
}

// AwaitIterationDrained blocks until any in-flight iterateTable goroutine
// for the given table name has returned. Returns immediately if no
// iteration is currently registered for the table. Used by the schema-change
// detector before issuing a bulk DELETE so concurrent INSERTs from an
// abandoned worker cannot survive the delete.
func (d *DataIterator) AwaitIterationDrained(tableName string) {
	d.iterationsMu.Lock()
	done, ok := d.iterations[tableName]
	d.iterationsMu.Unlock()
	if !ok {
		return
	}
	<-done
}

// RequeueTable resets state and spawns a fresh single-table iteration. Used
// by automatic DDL handling: after the schema-change detector clears the
// target rows for a table, it calls RequeueTable to repopulate from source's
// current state. Safe to call from a goroutine other than DataIterator.Run's
// worker pool.
//
// Caller is responsible for ensuring no in-flight worker is still copying
// this table. The detector achieves that by waiting for IsTableComplete to
// return true before deleting target rows.
func (d *DataIterator) RequeueTable(table *TableSchema) error {
	if d.logger == nil {
		d.logger = LogWithField("tag", "data_iterator")
	}
	logger := d.logger.WithField("table", table.String())

	d.StateTracker.ResetTable(table.String())

	tablesWithData, emptyTables, err := MaxPaginationKeys(d.DB, []*TableSchema{table}, d.logger)
	if err != nil {
		return fmt.Errorf("requeue: read max pagination key for %s: %w", table.String(), err)
	}

	if len(emptyTables) > 0 {
		// Source has no rows for this table — mark complete and notify.
		d.StateTracker.MarkTableAsCompleted(table.String())
		for _, listener := range d.tableCompletionListeners {
			listener(table)
		}
		logger.Info("requeue: source table empty; marked complete without iteration")
		return nil
	}

	maxKey, ok := tablesWithData[table]
	if !ok {
		return fmt.Errorf("requeue: %s missing from MaxPaginationKeys result", table.String())
	}
	d.TargetPaginationKeys.Store(table.String(), maxKey)

	logger.Info("requeue: spawning fresh iteration goroutine for table")
	if d.Tracer != nil {
		d.Tracer.Trace("RequeueTable goroutine_start table=%s max_pk=%s", table.String(), maxKey.String())
	}
	go d.iterateTable(table)
	return nil
}

func (d *DataIterator) AddBatchListener(listener func(*RowBatch) error) {
	d.batchListeners = append(d.batchListeners, listener)
}

func (d *DataIterator) AddDoneListener(listener func() error) {
	d.doneListeners = append(d.doneListeners, listener)
}

// AddTableCompletionListener registers a callback fired whenever a single
// table finishes iteration (initial copy or requeue).
func (d *DataIterator) AddTableCompletionListener(listener func(*TableSchema)) {
	d.tableCompletionListeners = append(d.tableCompletionListeners, listener)
}

// columnsDriftFromCache returns true when the live SELECT columns no longer
// match the cached TableSchema columns by name and order. The cursor's batch
// columns come straight from rows.Columns(), so this catches DDL on source
// before the QUERY_EVENT for that DDL has been processed.
func columnsDriftFromCache(liveColumns []string, cached *TableSchema) bool {
	if cached == nil || cached.Table == nil {
		return false
	}
	if len(liveColumns) != len(cached.Columns) {
		return true
	}
	for i, name := range liveColumns {
		if name != cached.Columns[i].Name {
			return true
		}
	}
	return false
}
