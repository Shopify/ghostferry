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
	BuildSelect     func([]string, *TableSchema, uint64, uint64) (squirrel.SelectBuilder, error)
	// BatchSize is a pointer to the BatchSize in Config.UpdatableConfig which can be independently updated from this code.
	// Having it as a pointer allows the updated value to be read without needing additional code to copy the batch size value into the cursor config for each cursor we create.
	BatchSize                 *uint64
	BatchSizePerTableOverride *DataIterationBatchSizePerTableOverride
	ReadRetries               int
}

// returns a new Cursor with an embedded copy of itself
func (c *CursorConfig) NewCursor(table *TableSchema, startPaginationKey, maxPaginationKey uint64) *Cursor {
	return &Cursor{
		CursorConfig:                *c,
		Table:                       table,
		MaxPaginationKey:            maxPaginationKey,
		RowLock:                     true,
		lastSuccessfulPaginationKey: startPaginationKey,
	}
}

// returns a new Cursor with an embedded copy of itself
func (c *CursorConfig) NewCursorWithoutRowLock(table *TableSchema, startPaginationKey, maxPaginationKey uint64) *Cursor {
	cursor := c.NewCursor(table, startPaginationKey, maxPaginationKey)
	cursor.RowLock = false
	return cursor
}

// NewCompositeCursor creates a cursor for composite key pagination
func (c *CursorConfig) NewCompositeCursor(table *TableSchema, startKeys, maxKeys CompositeKey) *Cursor {
	cursor := &Cursor{
		CursorConfig:               *c,
		Table:                      table,
		RowLock:                    true,
		isComposite:                true,
		lastSuccessfulCompositeKey: startKeys,
		maxCompositeKey:            maxKeys,
		// Set single key values from first column for backward compatibility
		MaxPaginationKey:            maxKeys.Values[0].(uint64),
		lastSuccessfulPaginationKey: startKeys.Values[0].(uint64),
	}
	return cursor
}

// NewCompositeCursorWithoutRowLock creates a cursor for composite key pagination without row locks
func (c *CursorConfig) NewCompositeCursorWithoutRowLock(table *TableSchema, startKeys, maxKeys CompositeKey) *Cursor {
	cursor := c.NewCompositeCursor(table, startKeys, maxKeys)
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

// CompositeKey represents a composite pagination key
type CompositeKey struct {
	Values []interface{} // Can be uint64 or string
}

// NewCompositeKey creates a CompositeKey from values
func NewCompositeKey(values ...interface{}) CompositeKey {
	return CompositeKey{Values: values}
}

// IsLessThan compares two composite keys
func (c CompositeKey) IsLessThan(other CompositeKey) bool {
	for i := 0; i < len(c.Values) && i < len(other.Values); i++ {
		switch v1 := c.Values[i].(type) {
		case uint64:
			v2 := other.Values[i].(uint64)
			if v1 < v2 {
				return true
			} else if v1 > v2 {
				return false
			}
		case string:
			v2 := other.Values[i].(string)
			if v1 < v2 {
				return true
			} else if v1 > v2 {
				return false
			}
		}
	}
	return false
}

type Cursor struct {
	CursorConfig

	Table            *TableSchema
	MaxPaginationKey uint64
	RowLock          bool

	// Single column pagination (backward compatibility)
	paginationKeyColumn         *schema.TableColumn
	lastSuccessfulPaginationKey uint64
	
	// Composite key pagination
	isComposite                     bool
	maxCompositeKey                 CompositeKey
	lastSuccessfulCompositeKey      CompositeKey
	
	logger                      *logrus.Entry
}

// shouldContinue checks if cursor should continue iterating
func (c *Cursor) shouldContinue() bool {
	if c.isComposite {
		return c.lastSuccessfulCompositeKey.IsLessThan(c.maxCompositeKey)
	}
	return c.lastSuccessfulPaginationKey < c.MaxPaginationKey
}

func (c *Cursor) Each(f func(*RowBatch) error) error {
	c.logger = logrus.WithFields(logrus.Fields{
		"table": c.Table.String(),
		"tag":   "cursor",
	})
	
	if !c.isComposite {
		c.paginationKeyColumn = c.Table.GetPaginationColumn()
	}

	if len(c.ColumnsToSelect) == 0 {
		c.ColumnsToSelect = []string{"*"}
	}

	// Use appropriate loop condition based on pagination type
	for c.shouldContinue() {
		var tx SqlPreparerAndRollbacker
		var batch *RowBatch
		var paginationKeypos uint64

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

		if paginationKeypos <= c.lastSuccessfulPaginationKey {
			tx.Rollback()
			err = fmt.Errorf("new paginationKeypos %d <= lastSuccessfulPaginationKey %d", paginationKeypos, c.lastSuccessfulPaginationKey)
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

		// Update pagination position
		if c.isComposite {
			c.updateCompositePosition(batch)
		} else {
			c.lastSuccessfulPaginationKey = paginationKeypos
		}
	}

	return nil
}

// updateCompositePosition updates the last successful composite key from the batch
func (c *Cursor) updateCompositePosition(batch *RowBatch) {
	if batch.Size() == 0 {
		return
	}
	
	lastRow := batch.Values()[batch.Size()-1]
	newKeys := make([]interface{}, len(c.Table.CompositePaginationIndexes))
	
	for i, idx := range c.Table.CompositePaginationIndexes {
		switch v := lastRow[idx].(type) {
		case int64:
			newKeys[i] = uint64(v)
		default:
			newKeys[i] = v
		}
	}
	
	c.lastSuccessfulCompositeKey = NewCompositeKey(newKeys...)
	// Update single key for backward compatibility
	if v, ok := newKeys[0].(uint64); ok {
		c.lastSuccessfulPaginationKey = v
	}
}

func (c *Cursor) Fetch(db SqlPreparer) (batch *RowBatch, paginationKeypos uint64, err error) {
	var selectBuilder squirrel.SelectBuilder
	batchSize := c.CursorConfig.GetBatchSize(c.Table.Schema, c.Table.Name)

	if c.isComposite {
		// Use composite pagination
		selectBuilder = DefaultBuildSelectComposite(c.ColumnsToSelect, c.Table, c.lastSuccessfulCompositeKey.Values, batchSize)
	} else if c.BuildSelect != nil {
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
			return nil, 0, err
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
	var compositePaginationIndexes []int
	
	if c.isComposite {
		// Find all composite pagination column indexes
		compositePaginationIndexes = make([]int, len(c.Table.CompositePaginationColumns))
		for i, paginationCol := range c.Table.CompositePaginationColumns {
			found := false
			for idx, col := range columns {
				if col == paginationCol.Name {
					compositePaginationIndexes[i] = idx
					if i == 0 {
						paginationKeyIndex = idx // First column for backward compatibility
					}
					found = true
					break
				}
			}
			if !found {
				err = fmt.Errorf("composite paginationKey column %s not found in columns: %v", paginationCol.Name, columns)
				logger.WithError(err).Error("failed to get composite paginationKey index")
				return
			}
		}
	} else {
		// Single pagination key
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
		paginationKeypos, err = batchData[len(batchData)-1].GetUint64(paginationKeyIndex)
		if err != nil {
			logger.WithError(err).Error("failed to get uint64 paginationKey value")
			return
		}
	}

	batch = &RowBatch{
		values:                    batchData,
		paginationKeyIndex:        paginationKeyIndex,
		isCompositePagination:     c.isComposite,
		compositePaginationIndexes: compositePaginationIndexes,
		table:                     c.Table,
		columns:                   columns,
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

// BuildCompositeTupleComparison creates a WHERE clause for composite key pagination
// For columns (a,b,c) and values (x,y,z), generates:
// WHERE a > x OR (a = x AND b > y) OR (a = x AND b = y AND c > z)
func BuildCompositeTupleComparison(columns []string, values []interface{}) squirrel.Or {
	conditions := make(squirrel.Or, 0, len(columns))
	
	for i := 0; i < len(columns); i++ {
		condition := squirrel.And{}
		
		// Add equality conditions for all columns before the current one
		for j := 0; j < i; j++ {
			condition = append(condition, squirrel.Eq{columns[j]: values[j]})
		}
		
		// Add greater than condition for the current column
		condition = append(condition, squirrel.Gt{columns[i]: values[i]})
		
		conditions = append(conditions, condition)
	}
	
	return conditions
}

func DefaultBuildSelect(columns []string, table *TableSchema, lastPaginationKey, batchSize uint64) squirrel.SelectBuilder {
	quotedPaginationKey := QuoteField(table.GetPaginationColumn().Name)

	return squirrel.Select(columns...).
		From(QuotedTableName(table)).
		Where(squirrel.Gt{quotedPaginationKey: lastPaginationKey}).
		Limit(batchSize).
		OrderBy(quotedPaginationKey)
}

// DefaultBuildSelectComposite builds a SELECT query for composite pagination
func DefaultBuildSelectComposite(columns []string, table *TableSchema, lastPaginationKeys []interface{}, batchSize uint64) squirrel.SelectBuilder {
	quotedColumns := make([]string, len(table.CompositePaginationColumns))
	orderByColumns := make([]string, len(table.CompositePaginationColumns))
	
	for i, col := range table.CompositePaginationColumns {
		quotedColumns[i] = QuoteField(col.Name)
		orderByColumns[i] = QuoteField(col.Name)
	}
	
	whereClause := BuildCompositeTupleComparison(quotedColumns, lastPaginationKeys)
	
	return squirrel.Select(columns...).
		From(QuotedTableName(table)).
		Where(whereClause).
		Limit(batchSize).
		OrderBy(orderByColumns...)
}
