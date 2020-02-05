package ghostferry

import (
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

type CursorAsync struct {
	*Cursor
	mux sync.RWMutex
}

// returns a new Cursor with an embedded copy of itself
func (c *CursorConfig) NewCursorAsync(table *TableSchema, startPaginationKey, maxPaginationKey uint64) *CursorAsync {
	cursor := c.NewCursor(table, startPaginationKey, maxPaginationKey)
	return &CursorAsync{
		Cursor: cursor,
	}
}

// Each iterates over all records for one single table
// Each cursor only handles one table at a time
// f is called for every row from fetch
func (c *CursorAsync) Each(concurrency int, f func(*RowBatch) error) error {
	c.logger = logrus.WithFields(logrus.Fields{
		"table": c.Table.String(),
		"tag":   "cursor",
	})
	c.paginationKeyColumn = c.Table.GetPaginationColumn()

	if len(c.ColumnsToSelect) == 0 {
		c.ColumnsToSelect = []string{"*"}
	}

	var wg sync.WaitGroup
	wg.Add(concurrency)
	inLastKey := make(chan uint64, concurrency)
	outLastKey := make(chan uint64, concurrency)
	outError := make(chan error, 1)
	done := make(chan bool, concurrency)

	startTime := time.Now()

	inLastKey <- 0

	for i := 0; i < concurrency; i++ {
		go func(index int, inLastKey <-chan uint64, outLastKey chan<- uint64, outError chan<- error) {
			jobCount := 0
			var key uint64
			var batchCompleted bool

			defer func() {
				c.logger.Println(fmt.Sprintf("WORKER(%d/%d) FINISHING GOROUTINE BATCHCOMPLETED %v", index, jobCount, batchCompleted))
				wg.Done()
			}()

			for {
				select {
				case lastKey := <-inLastKey:
					jobCount++
					key = lastKey
					batchCompleted = false
					startBatch := time.Now()

					c.logger.WithFields(logrus.Fields{
						"time":      time.Now().Format(time.RFC3339),
						"key":       key,
						"completed": batchCompleted,
					}).Println(fmt.Sprintf("WORKER(%d/%d) START PROCESSING", index, jobCount))
					var tx SqlPreparerAndRollbacker
					var batch *RowBatch
					var paginationKeyPos uint64

					err := WithRetries(c.ReadRetries, 60, c.logger, "fetch rows", func() (err error) {
						if c.Throttler != nil {
							WaitForThrottle(c.Throttler)
						}

						// Only need to use a transaction if RowLock == true. Otherwise
						// we'd be wasting two extra round trips per batch, doing
						// essentially a no-op.
						if c.RowLock {
							tx, err = c.DB.Begin()
							if err != nil {
								return err
							}
						} else {
							tx = &SqlDBWithFakeRollback{c.DB}
						}

						batch, paginationKeyPos, err = c.Fetch(tx, lastKey, c.MaxPaginationKey)
						if err == nil {
							return nil
						}

						tx.Rollback()
						return err
					})

					if err != nil {
						outError <- err
					}

					if paginationKeyPos != 0 && paginationKeyPos < c.lastSuccessfulPaginationKey {
						tx.Rollback()
						err = fmt.Errorf("new paginationKeyPos %d/%d <= lastSuccessfulPaginationKey %d", paginationKeyPos, key, c.lastSuccessfulPaginationKey)
						c.logger.WithError(err).Errorf("last successful paginationKey position did not advance")
						outError <- err
					}

					//Sends back the last pagination key from the batch
					outLastKey <- paginationKeyPos

					err = f(batch)
					if err != nil {
						tx.Rollback()
						c.logger.WithError(err).Error("failed to call each callback")
						outError <- err
					}

					tx.Rollback()

					//Just flag that the batch has been processed
					batchCompleted = true

					c.logger.WithFields(logrus.Fields{
						"time":      time.Since(startBatch).String(),
						"key":       key,
						"completed": batchCompleted,
						"worker":    index,
						"job":       jobCount,
					}).Println(fmt.Sprintf("WORKER(%d/%d) BATCH PROCESSED", index, jobCount))

				case <-done:
					c.logger.WithFields(logrus.Fields{
						"time":      time.Now().Format(time.RFC3339),
						"key":       key,
						"completed": batchCompleted,
						"worker":    index,
						"job":       jobCount,
					}).Println(fmt.Sprintf("WORKER (%d/%d) DONE", index, jobCount))
					return
				}
			}

		}(i, inLastKey, outLastKey, outError)
	}

ReadLastKey:
	for {
		select {
		case e := <-outError:
			c.logger.Fatal(e)
		case out := <-outLastKey:
			if out > c.lastSuccessfulPaginationKey {
				c.SetLastSuccessfulPaginationKey(out)
			}

			if out >= c.MaxPaginationKey {
				c.SetLastSuccessfulPaginationKey(out)
				c.logger.Println(fmt.Sprintf("CLOSING WORKERS MAXPAGINATION KEY: %d", out))
				for i := 0; i < concurrency; i++ {
					done <- true
				}
				break ReadLastKey
			} else {
				inLastKey <- out
			}
		}

	}

	c.logger.Println("WAITING FOR WORKERS TO FINISH")
	wg.Wait()

	c.logger.Println(fmt.Sprintf("TOTAL TIME: %s", time.Since(startTime)))

	return nil
}

func (c *CursorAsync) SetLastSuccessfulPaginationKey(key uint64) {
	defer c.mux.Unlock()

	c.mux.Lock()
	c.lastSuccessfulPaginationKey = key
}

func (c *CursorAsync) Fetch(db SqlPreparer, lastSuccessfulPaginationKey, maxPaginationKey uint64) (batch *RowBatch, paginationKeypos uint64, err error) {
	var selectBuilder squirrel.SelectBuilder

	if c.BuildSelect != nil {
		selectBuilder, err = c.BuildSelect(c.ColumnsToSelect, c.Table, lastSuccessfulPaginationKey, c.BatchSize)
		if err != nil {
			c.logger.WithError(err).Error("failed to apply filter for select")
			return
		}
	} else {
		selectBuilder = BuildSelectWithRange(c.ColumnsToSelect, c.Table, lastSuccessfulPaginationKey, maxPaginationKey, c.BatchSize)
	}

	if c.RowLock {
		selectBuilder = selectBuilder.Suffix("FOR UPDATE")
	}

	query, args, err := selectBuilder.ToSql()
	if err != nil {
		c.logger.WithError(err).Error("failed to build chunking sql")
		return
	}

	// With the inline verifier, the columns to be selected may be very large as
	// the query generated will be very large. The code here simply hides the
	// columns from the logger to not spam the logs.

	splitQuery := strings.Split(query, "FROM")
	loggedQuery := fmt.Sprintf("SELECT [omitted] FROM %s", splitQuery[1])

	logger := c.logger.WithFields(logrus.Fields{
		"sql":  loggedQuery,
		"args": args,
	})

	// This query must be a prepared query. If it is not, querying will use
	// MySQL's plain text interface, which will scan all values into []uint8
	// if we give it []interface{}.
	stmt, err := db.Prepare(query)
	if err != nil {
		logger.WithError(err).Error("failed to prepare query")
		return
	}

	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		logger.WithError(err).Error("failed to query database")
		return
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		logger.WithError(err).Error("failed to get columns")
		return
	}

	var paginationKeyIndex int = -1
	for idx, col := range columns {
		if col == c.paginationKeyColumn.Name {
			paginationKeyIndex = idx
			break
		}
	}

	if paginationKeyIndex < 0 {
		err = fmt.Errorf("paginationKey is not found during iteration with columns: %v", columns)
		logger.WithError(err).Error("failed to get paginationKey index")
		return
	}

	var rowData RowData
	var batchData []RowData

	for rows.Next() {
		rowData, err = ScanGenericRow(rows, len(columns))
		if err != nil {
			logger.WithError(err).Error("failed to scan row")
			return
		}

		batchData = append(batchData, rowData)
	}

	err = rows.Err()
	if err != nil {
		return
	}

	if len(batchData) > 0 {
		// TODO this is where the last key from the batch is found
		paginationKeypos, err = batchData[len(batchData)-1].GetUint64(paginationKeyIndex)
		if err != nil {
			logger.WithError(err).Error("failed to get uint64 paginationKey value")
			return
		}
	}

	batch = &RowBatch{
		values:             batchData,
		paginationKeyIndex: paginationKeyIndex,
		table:              c.Table,
	}

	logger.Debugf("found %d rows", batch.Size())

	return
}

func BuildSelectWithRange(columns []string, table *TableSchema, lastPaginationKey, maxPaginationKey, batchSize uint64) squirrel.SelectBuilder {
	quotedPaginationKey := quoteField(table.GetPaginationColumn().Name)

	return squirrel.Select(columns...).
		From(QuotedTableName(table)).
		Where(squirrel.Gt{quotedPaginationKey: lastPaginationKey}).
		Where(squirrel.LtOrEq{quotedPaginationKey: maxPaginationKey}).
		Limit(batchSize).
		OrderBy(quotedPaginationKey)
}
