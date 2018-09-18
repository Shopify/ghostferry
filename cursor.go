package ghostferry

import (
	"database/sql"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

// both `sql.Tx` and `sql.DB` allow a SQL query to be `Prepare`d
type SqlPreparer interface {
	Prepare(string) (*sql.Stmt, error)
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
	BuildSelect     func([]string, *schema.Table, uint64, uint64) (squirrel.SelectBuilder, error)
	BatchSize       uint64
	ReadRetries     int
}

// returns a new Cursor with an embedded copy of itself
func (c *CursorConfig) NewCursor(table *schema.Table, startPk, maxPk uint64) *Cursor {
	return &Cursor{
		CursorConfig:             *c,
		Table:                    table,
		MaxPrimaryKey:            maxPk,
		RowLock:                  true,
		lastSuccessfulPrimaryKey: startPk,
	}
}

// returns a new Cursor with an embedded copy of itself
func (c *CursorConfig) NewCursorWithoutRowLock(table *schema.Table, startPk, maxPk uint64) *Cursor {
	return &Cursor{
		CursorConfig:             *c,
		Table:                    table,
		MaxPrimaryKey:            maxPk,
		RowLock:                  false,
		lastSuccessfulPrimaryKey: startPk,
	}
}

type Cursor struct {
	CursorConfig

	Table         *schema.Table
	MaxPrimaryKey uint64
	RowLock       bool

	pkColumn                 *schema.TableColumn
	lastSuccessfulPrimaryKey uint64
	logger                   *logrus.Entry
}

func (c *Cursor) Each(f func(*RowBatch) error) error {
	c.logger = logrus.WithFields(logrus.Fields{
		"table": c.Table.String(),
		"tag":   "cursor",
	})
	c.pkColumn = c.Table.GetPKColumn(0)

	if len(c.ColumnsToSelect) == 0 {
		c.ColumnsToSelect = []string{"*"}
	}

	for c.lastSuccessfulPrimaryKey < c.MaxPrimaryKey {
		var tx SqlPreparerAndRollbacker
		var batch *RowBatch
		var pkpos uint64

		err := WithRetries(c.ReadRetries, 0, c.logger, "fetch rows", func() (err error) {
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

			batch, pkpos, err = c.Fetch(tx)
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

		if pkpos <= c.lastSuccessfulPrimaryKey {
			tx.Rollback()
			err = fmt.Errorf("new pkpos %d <= lastSuccessfulPk %d", pkpos, c.lastSuccessfulPrimaryKey)
			c.logger.WithError(err).Errorf("last successful pk position did not advance")
			return err
		}

		err = f(batch)
		if err != nil {
			tx.Rollback()
			c.logger.WithError(err).Error("failed to call each callback")
			return err
		}

		tx.Rollback()

		c.lastSuccessfulPrimaryKey = pkpos
	}

	return nil
}

func (c *Cursor) Fetch(db SqlPreparer) (batch *RowBatch, pkpos uint64, err error) {
	var selectBuilder squirrel.SelectBuilder

	if c.BuildSelect != nil {
		selectBuilder, err = c.BuildSelect(c.ColumnsToSelect, c.Table, c.lastSuccessfulPrimaryKey, c.BatchSize)
		if err != nil {
			c.logger.WithError(err).Error("failed to apply filter for select")
			return
		}
	} else {
		selectBuilder = DefaultBuildSelect(c.ColumnsToSelect, c.Table, c.lastSuccessfulPrimaryKey, c.BatchSize)
	}

	if c.RowLock {
		selectBuilder = selectBuilder.Suffix("FOR UPDATE")
	}

	query, args, err := selectBuilder.ToSql()
	if err != nil {
		c.logger.WithError(err).Error("failed to build chunking sql")
		return
	}

	logger := c.logger.WithFields(logrus.Fields{
		"sql":  query,
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

	var pkIndex int = -1
	for idx, col := range columns {
		if col == c.pkColumn.Name {
			pkIndex = idx
			break
		}
	}

	if pkIndex < 0 {
		err = fmt.Errorf("pk is not found during iteration with columns: %v", columns)
		logger.WithError(err).Error("failed to get pk index")
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
		pkpos, err = batchData[len(batchData)-1].GetUint64(pkIndex)
		if err != nil {
			logger.WithError(err).Error("failed to get uint64 pk value")
			return
		}
	}

	batch = NewRowBatch(c.Table, batchData, pkIndex)

	logger.Debugf("found %d rows", batch.Size())

	return
}

func ScanGenericRow(rows *sql.Rows, columnCount int) (RowData, error) {
	values := make(RowData, columnCount)
	valuePtrs := make(RowData, columnCount)

	for i, _ := range values {
		valuePtrs[i] = &values[i]
	}

	err := rows.Scan(valuePtrs...)
	return values, err
}

func ScanByteRow(rows *sql.Rows, columnCount int) ([][]byte, error) {
	values := make([][]byte, columnCount)
	valuePtrs := make(RowData, columnCount)

	for i, _ := range values {
		valuePtrs[i] = &values[i]
	}

	err := rows.Scan(valuePtrs...)
	return values, err
}

func DefaultBuildSelect(columns []string, table *schema.Table, lastPk, batchSize uint64) squirrel.SelectBuilder {
	quotedPK := quoteField(table.GetPKColumn(0).Name)

	return squirrel.Select(columns...).
		From(QuotedTableName(table)).
		Where(squirrel.Gt{quotedPK: lastPk}).
		Limit(batchSize).
		OrderBy(quotedPK)
}
