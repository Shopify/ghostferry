package ghostferry

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type DataIterator struct {
	DB          *sql.DB
	Tables      []*schema.Table
	Concurrency int

	ErrorHandler ErrorHandler
	CursorConfig *CursorConfig
	StateTracker *StateTracker

	targetPKs      *sync.Map
	batchListeners []func(*RowBatch) error
	doneListeners  []func() error
	logger         *logrus.Entry
}

func (d *DataIterator) Initialize() error {
	d.logger = logrus.WithField("tag", "data_iterator")
	d.targetPKs = &sync.Map{}

	if d.StateTracker == nil {
		return errors.New("StateTracker must be defined")
	}

	return nil
}

func (d *DataIterator) Run() {
	d.logger.WithField("tablesCount", len(d.Tables)).Info("starting data iterator run")

	tablesWithData, emptyTables, err := MaxPrimaryKeys(d.DB, d.Tables, d.logger)
	if err != nil {
		d.ErrorHandler.Fatal("data_iterator", err)
		return
	}

	for _, table := range emptyTables {
		d.StateTracker.MarkTableAsCompleted(table.String())
	}

	for table, maxPk := range tablesWithData {
		d.targetPKs.Store(table.String(), maxPk)
	}

	tablesQueue := make(chan *schema.Table)
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

				cursor := d.CursorConfig.NewCursor(table, targetPKInterface.(uint64))
				err := cursor.Each(func(batch *RowBatch) error {
					metrics.Count("RowEvent", int64(batch.Size()), []MetricTag{
						MetricTag{"table", table.Name},
						MetricTag{"source", "table"},
					}, 1.0)

					for _, listener := range d.batchListeners {
						err := listener(batch)
						if err != nil {
							logger.WithError(err).Error("failed to process row batch with listeners")
							return err
						}
					}

					// The way we save the LastSuccessfulPK is probably incorrect if we
					// want to ensure that when we crash, we have a "correct" view of
					// the LastSuccessfulPK.
					// However, it's uncertain if it is even theoretically possible to
					// save the "correct" value.
					// TODO: investigate this if we want to ensure that on error, we have
					//       the "correct" last successful PK and other values.
					// TODO: it is also perhaps possible to save the Cursor objects
					// directly as opposed to saving a state, but that is left to
					// the future.
					lastRow := batch.Values()[len(batch.Values())-1]
					pkpos, err := lastRow.GetUint64(batch.PkIndex())
					if err != nil {
						logger.WithError(err).Error("failed to convert pk to uint64")
						return err
					}

					logger.Debugf("updated last successful PK to %d", pkpos)
					d.StateTracker.UpdateLastSuccessfulPK(table.String(), pkpos)

					return nil
				})

				if err != nil {
					logger.WithError(err).Error("failed to iterate table")
					d.ErrorHandler.Fatal("data_iterator", err)
					return
				}

				logger.Debug("table iteration completed")
				d.StateTracker.MarkTableAsCompleted(table.String())
			}
		}()
	}

	for table, _ := range tablesWithData {
		tablesQueue <- table
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
