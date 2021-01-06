package ghostferry

import (
	"fmt"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"math"
	"sync"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
)

type DataIterator struct {
	DB                *sql.DB
	Concurrency       int
	SelectFingerprint bool

	ErrorHandler ErrorHandler
	CursorConfig *CursorConfig
	StateTracker *StateTracker

	targetPaginationKeys *sync.Map
	batchListeners       []func(*RowBatch) error
	doneListeners        []func() error
	logger               *logrus.Entry
}

type TableKeyInfo struct {
	Table *TableSchema
	MaxPaginationKey uint64
}

func (d *DataIterator) Run(tables []*TableSchema) {
	d.logger = logrus.WithField("tag", "data_iterator")
	d.targetPaginationKeys = &sync.Map{}

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
			d.targetPaginationKeys.Store(tableName, maxPaginationKey)
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

				targetPaginationKeyInterface, found := d.targetPaginationKeys.Load(table.String())
				if !found {
					err := fmt.Errorf("%s not found in targetPaginationKeys, this is likely a programmer error", table.String())
					logger.WithError(err).Error("this is definitely a bug")
					d.ErrorHandler.Fatal("data_iterator", err)
					return
				}

				startPaginationKey := d.StateTracker.LastSuccessfulPaginationKey(table.String())
				if startPaginationKey == math.MaxUint64 {
					err := fmt.Errorf("%v has been marked as completed but a table iterator has been spawned, this is likely a programmer error which resulted in the inconsistent starting state", table.String())
					logger.WithError(err).Error("this is definitely a bug")
					d.ErrorHandler.Fatal("data_iterator", err)
					return
				}

				cursor := d.CursorConfig.NewCursor(table, startPaginationKey, targetPaginationKeyInterface.(uint64))
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
							paginationKey, err := rowData.GetUint64(batch.PaginationKeyIndex())
							if err != nil {
								logger.WithError(err).Error("failed to get paginationKey data")
								return err
							}

							fingerprints[paginationKey] = rowData[len(rowData)-1].([]byte)
							rows[i] = rowData[:len(rowData)-1]
						}

						batch = &RowBatch{
							values:             rows,
							paginationKeyIndex: batch.PaginationKeyIndex(),
							table:              table,
							fingerprints:       fingerprints,
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

	sortedTableData, err := GetOrderedTables(tablesWithData, d.DB)

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

func (d *DataIterator) AddBatchListener(listener func(*RowBatch) error) {
	d.batchListeners = append(d.batchListeners, listener)
}

func (d *DataIterator) AddDoneListener(listener func() error) {
	d.doneListeners = append(d.doneListeners, listener)
}

func GetOrderedTables(unorderedTables map[*TableSchema]uint64, db *sql.DB) ([]TableKeyInfo, error) {
	orderedTables := make([]TableKeyInfo, len(unorderedTables))
	i := 0

	for k, v := range unorderedTables {
		orderedTables[i] = TableKeyInfo{k, v}
		i++
	}

	tableNames := make([]string, len(unorderedTables))
	databaseSchemasMap := make(map[string]int, len(unorderedTables))

	i = 0
	for t := range unorderedTables {
			tableNames[i] = t.Name
			databaseSchemasMap[t.Schema] = 0
			i++
	}

	databaseSchemas := make([]string, len(databaseSchemasMap))

	i = 0
	for schema := range databaseSchemasMap {
		databaseSchemas[i] = schema
	}

	query := fmt.Sprintf("SELECT TABLE_NAME, TABLE_SCHEMA, DATA_LENGTH FROM information_schema.tables WHERE TABLE_SCHEMA IN (\"%s\") AND TABLE_NAME IN (\"%s\") ORDER BY DATA_LENGTH DESC", strings.Join(databaseSchemas, `", "`), strings.Join(tableNames, `", "`))
	rows, err := db.Query(query)

	if err != nil {
		return orderedTables, err
	}

	defer rows.Close()

	databaseOrder := make(map[string]int, len(unorderedTables))
	i = 0
	for rows.Next() {
		var tableName, schemaName string
		var size uint64
		err = rows.Scan(&tableName, &schemaName, &size)

		if err != nil {
			return orderedTables, err
		}

		databaseOrder[fmt.Sprintf("%s %s", tableName, schemaName)] = i
		i++
	}

	sort.Slice(orderedTables, func(i, j int) bool {
		iName := fmt.Sprintf("%s %s", orderedTables[i].Table.Name, orderedTables[i].Table.Schema)
		jName := fmt.Sprintf("%s %s", orderedTables[j].Table.Name, orderedTables[j].Table.Schema)
		return databaseOrder[iName] < databaseOrder[jName]
	})

	return orderedTables, nil
}
