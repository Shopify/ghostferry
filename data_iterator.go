package ghostferry

import (
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"math"

	"github.com/sirupsen/logrus"
)

type DataIteratorBatch struct {
	table          *TableSchema
	paginationKeys *MinMaxKeys
}

func (p *DataIteratorBatch) BatchID() uint64 {
	return p.paginationKeys.MinPaginationKey
}

type DataIterator struct {
	DB                *sql.DB
	Concurrency       int
	SelectFingerprint bool

	ErrorHandler ErrorHandler
	CursorConfig *CursorConfig
	StateTracker *StateTracker

	batches        map[string]*DataIteratorBatch
	batchListeners []func(*RowBatch) error
	doneListeners  []func() error
	logger         *logrus.Entry
}

func (d *DataIterator) Run(tables []*TableSchema) {
	d.logger = logrus.WithField("tag", "data_iterator")

	// If a state tracker is not provided, then the caller doesn't care about
	// tracking state. However, some methods are still useful so we initialize
	// a minimal local instance.
	if d.StateTracker == nil {
		d.StateTracker = NewStateTracker()
	}

	d.logger.WithField("tablesCount", len(tables)).Info("starting data iterator run")
	tablesWithData, emptyTables, err := MinMaxPaginationKeys(d.DB, tables, d.logger)
	if err != nil {
		d.ErrorHandler.Fatal("data_iterator", err)
	}

	for _, table := range emptyTables {
		d.StateTracker.MarkTableAsCompleted(table.String())
	}

	d.batches = make(map[string]*DataIteratorBatch, len(tablesWithData))

	for table, keys := range tablesWithData {
		tableName := table.String()
		if d.StateTracker.IsTableComplete(tableName) {
			// In a previous run, the table may have been completed.
			// We don't need to reiterate those tables as it has already been done.
			d.logger.WithField("table", tableName).Warn("table already completed, skipping")
			continue
		}
		d.batches[tableName] = &DataIteratorBatch{table: table, paginationKeys: keys}
	}

	batchQueue := make(chan *DataIteratorBatch)

	for i := 0; i < d.Concurrency; i++ {
		go func() {
			for {
				batch, ok := <-batchQueue
				if !ok {
					break
				}

				batchLogger := d.logger.WithFields(logrus.Fields{
					"table":              batch.table.String(),
					"startPaginationKey": batch.paginationKeys.MinPaginationKey,
					"endPaginationKey":   batch.paginationKeys.MaxPaginationKey,
					"batchID":            batch.BatchID(),
				})

				batchLogger.Debug("setting up new batch cursor")
				cursor := d.CursorConfig.NewCursor(batch.table, batch.paginationKeys.MinPaginationKey, batch.paginationKeys.MaxPaginationKey)
				if d.SelectFingerprint {
					if len(cursor.ColumnsToSelect) == 0 {
						cursor.ColumnsToSelect = []string{"*"}
					}

					cursor.ColumnsToSelect = append(cursor.ColumnsToSelect, batch.table.RowMd5Query())
				}

				err := cursor.Each(func(batch *RowBatch) error {
					batchValues := batch.Values()
					paginationKeyIndex := batch.PaginationKeyIndex()

					batch.batchID = batch.BatchID()

					batchLogger.WithField("size", batch.Size()).Debug("row event")
					metrics.Count("RowEvent", int64(batch.Size()), []MetricTag{
						MetricTag{"table", batch.table.Name},
						MetricTag{"source", "table"},
					}, 1.0)

					if d.SelectFingerprint {
						fingerprints := make(map[uint64][]byte)
						rows := make([]RowData, batch.Size())

						for i, rowData := range batchValues {
							batch, err := rowData.GetUint64(paginationKeyIndex)
							if err != nil {
								batchLogger.WithError(err).Error("failed to get batch data")
								return err
							}

							fingerprints[batch] = rowData[len(rowData)-1].([]byte)
							rows[i] = rowData[:len(rowData)-1]
						}

						batch.values = rows
						batch.fingerprints = fingerprints
					}

					for _, listener := range d.batchListeners {
						err := listener(batch)
						if err != nil {
							batchLogger.WithError(err).Error("failed to process row batch with listeners")
							return err
						}
					}

					return nil
				})

				if err != nil {
					switch e := err.(type) {
					case BatchWriterVerificationFailed:
						d.logger.WithField("incorrect_tables", e.table).Error(e.Error())
						d.ErrorHandler.Fatal("inline_verifier", err)
					default:
						d.logger.WithError(err).Error("failed to iterate table")
						d.ErrorHandler.Fatal("data_iterator", err)
					}
				}

				batchLogger.Info("batch successfully copied")
			}
		}()
	}

	for _, batch := range d.batches {
		tableName := batch.table.String()
		stateBatches, loadBatchesFromState := d.StateTracker.batchProgress[tableName]

		if loadBatchesFromState {
			for _, stateBatch := range stateBatches {
				if stateBatch.Completed {
					continue
				}

				batchQueue <- &DataIteratorBatch{
					table: batch.table,
					paginationKeys: &MinMaxKeys{
						MinPaginationKey: stateBatch.LatestPaginationKey,
						MaxPaginationKey: stateBatch.EndPaginationKey,
					},
				}
			}
		} else {
			// Set start to minus one since cursor is searching for greater values
			tableStartPaginationKey := batch.paginationKeys.MinPaginationKey - 1
			tableEndPaginationKey := batch.paginationKeys.MaxPaginationKey

			// Number of batches are set to number of processes, unless each batch becomes smaller than the cursor size
			keyInterval := tableEndPaginationKey - tableStartPaginationKey
			concurrencyBatchSize := math.Ceil(float64(keyInterval) / float64(d.Concurrency))
			batchSize := uint64(math.Max(concurrencyBatchSize, float64(d.CursorConfig.BatchSize)))

			d.logger.WithFields(logrus.Fields{
				"table":              tableName,
				"batchSize":          batchSize,
				"endPaginationKey":   tableEndPaginationKey,
				"startPaginationKey": tableStartPaginationKey,
			}).Debugf("queueing %d batches", (keyInterval/batchSize)+1)

			for batchStartPaginationKey := tableStartPaginationKey; batchStartPaginationKey < tableEndPaginationKey; batchStartPaginationKey += batchSize {
				batchEndPaginationKey := batchStartPaginationKey + batchSize

				// Set batchEndPaginationKey to endPaginationKey if out of bounds.
				// batchEndPaginationKey is paginated with batchSize, this clause help us
				// set the proper endPaginationKey on last iteration.
				if batchEndPaginationKey > tableEndPaginationKey {
					batchEndPaginationKey = tableEndPaginationKey
				}

				batch := &DataIteratorBatch{
					table: batch.table,
					paginationKeys: &MinMaxKeys{
						MinPaginationKey: batchStartPaginationKey,
						MaxPaginationKey: batchEndPaginationKey,
					},
				}

				d.StateTracker.RegisterBatch(tableName, batch.BatchID(), batch.paginationKeys.MinPaginationKey, batch.paginationKeys.MaxPaginationKey)
				batchQueue <- batch

				// Protect against uint64 overflow, this might happen if the table is full.
				//
				// In a table with `^uint64(0)` records, `batchStartPaginationKey + batchSize` from previous iteration
				// might give us a `batchStartPaginationKey` that is larger than what uint64 can store.
				// Golang will in this case reset `batchStartPaginationKey` to `0 + remaining sum` causing an
				// infinite loop.
				if batchStartPaginationKey >= (^uint64(0) - batchSize) {
					batchSize = ^uint64(0) - batchStartPaginationKey
					if batchSize == 0 {
						break
					}
				}
			}
		}
	}

	d.logger.Info("done queueing tables to be iterated")
	close(batchQueue)

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
