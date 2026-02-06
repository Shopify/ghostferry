package ghostferry

import (
	sqlorig "database/sql"
	"fmt"
	"strings"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/Masterminds/squirrel"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

// both `sql.Tx` and `sql.DB` allow a SQL query to be `Prepare`d
type SqlPreparer interface {
	Prepare(string) (*sqlorig.Stmt, error)
}

type SqlDBWithFakeRollback struct {
	*sql.DB
}

func (d *SqlDBWithFakeRollback) Rollback() error {
	return nil
}

// sql.DB does not implement Rollback, but can use SqlDBWithFakeRollback
// to perform a noop.
type SqlPreparerAndRollbacker interface {
	SqlPreparer
	Rollback() error
}

type CursorConfig struct {
	DB        *sql.DB
	Throttler Throttler

	ColumnsToSelect []string
	BuildSelect     func([]string, *TableSchema, PaginationKey, uint64) (squirrel.SelectBuilder, error)
	// BatchSize is a pointer to the BatchSize in Config.UpdatableConfig which can be independently updated from this code.
	// Having it as a pointer allows the updated value to be read without needing additional code to copy the batch size value into the cursor config for each cursor we create.
	BatchSize                 *uint64
	BatchSizePerTableOverride *DataIterationBatchSizePerTableOverride
	ReadRetries               int
}

// returns a new Cursor with an embedded copy of itself
func (c *CursorConfig) NewCursor(table *TableSchema, startPaginationKey, maxPaginationKey PaginationKey) *Cursor {
	return &Cursor{
		CursorConfig:                *c,
		Table:                       table,
		MaxPaginationKey:            maxPaginationKey,
		RowLock:                     true,
		lastSuccessfulPaginationKey: startPaginationKey,
	}
}

// returns a new Cursor with an embedded copy of itself
func (c *CursorConfig) NewCursorWithoutRowLock(table *TableSchema, startPaginationKey, maxPaginationKey PaginationKey) *Cursor {
	cursor := c.NewCursor(table, startPaginationKey, maxPaginationKey)
	cursor.RowLock = false
	return cursor
}

func (c CursorConfig) GetBatchSize(schemaName string, tableName string) uint64 {
	if c.BatchSizePerTableOverride != nil {
		if batchSize, found := c.BatchSizePerTableOverride.TableOverride[schemaName][tableName]; found {
			return batchSize
		}
	}
	return *c.BatchSize
}

type Cursor struct {
	CursorConfig

	Table            *TableSchema
	MaxPaginationKey PaginationKey
	RowLock          bool

	paginationKeyColumn         *schema.TableColumn
	lastSuccessfulPaginationKey PaginationKey
	logger                      *logrus.Entry
}

func (c *Cursor) Each(f func(*RowBatch) error) error {
	c.logger = logrus.WithFields(logrus.Fields{
		"table": c.Table.String(),
		"tag":   "cursor",
	})
	c.paginationKeyColumn = c.Table.GetPaginationColumn()

	if len(c.ColumnsToSelect) == 0 {
		c.ColumnsToSelect = []string{"*"}
	}

	for c.lastSuccessfulPaginationKey.Compare(c.MaxPaginationKey) < 0 {
		var tx SqlPreparerAndRollbacker
		var batch *RowBatch
		var paginationKeypos PaginationKey

		err := WithRetries(c.ReadRetries, 1*time.Second, c.logger, "fetch rows", func() (err error) {
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

			batch, paginationKeypos, err = c.Fetch(tx)
			if err == nil {
				return nil
			}

			tx.Rollback()
			return err
		})

		if err != nil {
			return err
		}

		if batch.Size() == 0 {
			tx.Rollback()
			c.logger.Debug("did not reach max primary key, but the table is complete as there are no more rows")
			break
		}

		if paginationKeypos.Compare(c.lastSuccessfulPaginationKey) <= 0 {
			tx.Rollback()
			err = fmt.Errorf("new paginationKeypos %s <= lastSuccessfulPaginationKey %s", paginationKeypos.String(), c.lastSuccessfulPaginationKey.String())
			c.logger.WithError(err).Errorf("last successful paginationKey position did not advance")
			return err
		}

		err = f(batch)
		if err != nil {
			tx.Rollback()
			c.logger.WithError(err).Error("failed to call each callback")
			return err
		}

		tx.Rollback()

		c.lastSuccessfulPaginationKey = paginationKeypos
	}

	return nil
}

func (c *Cursor) Fetch(db SqlPreparer) (batch *RowBatch, paginationKeypos PaginationKey, err error) {
	var selectBuilder squirrel.SelectBuilder
	batchSize := c.CursorConfig.GetBatchSize(c.Table.Schema, c.Table.Name)

	if c.BuildSelect != nil {
		selectBuilder, err = c.BuildSelect(c.ColumnsToSelect, c.Table, c.lastSuccessfulPaginationKey, batchSize)
		if err != nil {
			c.logger.WithError(err).Error("failed to apply filter for select")
			return
		}
	} else {
		selectBuilder = DefaultBuildSelect(c.ColumnsToSelect, c.Table, c.lastSuccessfulPaginationKey, batchSize)
	}

	if c.RowLock {
		mySqlVersion, err := c.DB.QueryMySQLVersion()
		if err != nil {
			return nil, NewUint64Key(0), err
		}
		if strings.HasPrefix(mySqlVersion, "8.") {
			selectBuilder = selectBuilder.Suffix("FOR SHARE NOWAIT")
		} else {
			selectBuilder = selectBuilder.Suffix("LOCK IN SHARE MODE")
		}
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
		lastRowData := batchData[len(batchData)-1]
		paginationKeypos, err = NewPaginationKeyFromRow(lastRowData, paginationKeyIndex, c.paginationKeyColumn)
		if err != nil {
			logger.WithError(err).Error("failed to get paginationKey value")
			return
		}
	}

	batch = &RowBatch{
		values:             batchData,
		paginationKeyIndex: paginationKeyIndex,
		table:              c.Table,
		columns:            columns,
	}

	logger.Debugf("found %d rows", batch.Size())

	return
}

func ScanGenericRow(rows *sqlorig.Rows, columnCount int) (RowData, error) {
	values := make(RowData, columnCount)
	valuePtrs := make(RowData, columnCount)

	for i, _ := range values {
		valuePtrs[i] = &values[i]
	}

	err := rows.Scan(valuePtrs...)
	return values, err
}

func ScanByteRow(rows *sqlorig.Rows, columnCount int) ([][]byte, error) {
	values := make([][]byte, columnCount)
	valuePtrs := make(RowData, columnCount)

	for i, _ := range values {
		valuePtrs[i] = &values[i]
	}

	err := rows.Scan(valuePtrs...)
	return values, err
}

func DefaultBuildSelect(columns []string, table *TableSchema, lastPaginationKey PaginationKey, batchSize uint64) squirrel.SelectBuilder {
	quotedPaginationKey := QuoteField(table.GetPaginationColumn().Name)

	return squirrel.Select(columns...).
		From(QuotedTableName(table)).
		Where(squirrel.Gt{quotedPaginationKey: lastPaginationKey.SQLValue()}).
		Limit(batchSize).
		OrderBy(quotedPaginationKey)
}
