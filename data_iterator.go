package ghostferry

import (
	"database/sql"
	"fmt"
	"math"
	"sync"

	"github.com/sirupsen/logrus"
)

type DataIterator struct {
	DB                *sql.DB
	Concurrency       int
	SelectFingerprint bool

	ErrorHandler ErrorHandler
	CursorConfig *CursorConfig
	StateTracker *StateTracker

	targetPKs      *sync.Map
	batchListeners []func(*RowBatch) error
	doneListeners  []func() error
	logger         *logrus.Entry
}

func (d *DataIterator) Run(tables []*TableSchema) {
	d.logger = logrus.WithField("tag", "data_iterator")
	d.targetPKs = &sync.Map{}

	// If a state tracker is not provided, then the caller doesn't care about
	// tracking state. However, some methods are still useful so we initialize
	// a minimal local instance.
	if d.StateTracker == nil {
		d.StateTracker = NewStateTracker(0)
	}

	d.logger.WithField("tablesCount", len(tables)).Info("starting data iterator run")
	tablesWithData, emptyTables, err := MaxPrimaryKeys(d.DB, tables, d.logger)
	if err != nil {
		d.ErrorHandler.Fatal("data_iterator", err)
	}

	for _, table := range emptyTables {
		d.StateTracker.MarkTableAsCompleted(table.String())
	}

	for table, maxPk := range tablesWithData {
		tableName := table.String()
		if d.StateTracker.IsTableComplete(tableName) {
			// In a previous run, the table may have been completed.
			// We don't need to reiterate those tables as it has already been done.
			delete(tablesWithData, table)
		} else {
			d.targetPKs.Store(table.String(), maxPk)
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

				logger := d.logger.WithField("table", table.String())

				targetPKInterface, found := d.targetPKs.Load(table.String())
				if !found {
					err := fmt.Errorf("%s not found in targetPKs, this is likely a programmer error", table.String())
					logger.WithError(err).Error("this is definitely a bug")
					d.ErrorHandler.Fatal("data_iterator", err)
					return
				}

				startPk := d.StateTracker.LastSuccessfulPK(table.String())
				if startPk == math.MaxUint64 {
					err := fmt.Errorf("%v has been marked as completed but a table iterator has been spawned, this is likely a programmer error which resulted in the inconsistent starting state", table.String())
					logger.WithError(err).Error("this is definitely a bug")
					d.ErrorHandler.Fatal("data_iterator", err)
					return
				}

				cursor := d.CursorConfig.NewCursor(table, startPk, targetPKInterface.(uint64))
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
						fingerprints := make(map[uint64][]byte)
						rows := make([]RowData, batch.Size())

						for i, rowData := range batch.Values() {
							pk, err := rowData.GetUint64(batch.PkIndex())
							if err != nil {
								logger.WithError(err).Error("failed to get pk data")
								return err
							}

							fingerprints[pk] = rowData[len(rowData)-1].([]byte)
							rows[i] = rowData[:len(rowData)-1]
						}

						batch = &RowBatch{
							values:       rows,
							pkIndex:      batch.PkIndex(),
							table:        table,
							fingerprints: fingerprints,
						}
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

				// Right now the BatchWriter.WriteRowBatch happens synchronously in
				// this method. If it ever becomes async, this MarkTableAsCompleted
				// call MUST be done in WriteRowBatch somehow.
				d.StateTracker.MarkTableAsCompleted(table.String())
			}
		}()
	}

	i := 0
	loggingIncrement := len(tablesWithData) / 50
	if loggingIncrement == 0 {
		loggingIncrement = 1
	}

	for table, _ := range tablesWithData {
		tablesQueue <- table
		i++
		if i%loggingIncrement == 0 {
			d.logger.WithField("table", table.String()).Infof("queued table for processing (%d/%d)", i, len(tablesWithData))
		}
	}

	d.logger.Info("done queueing tables to be iterated, closing table channel")
	close(tablesQueue)

	wg.Wait()
	for _, listener := range d.doneListeners {
		listener()
	}
}

func (d *DataIterator) AddBatchListener(listener func(*RowBatch) error) {
	d.batchListeners = append(d.batchListeners, listener)
}

func (d *DataIterator) AddDoneListener(listener func() error) {
	d.doneListeners = append(d.doneListeners, listener)
}
