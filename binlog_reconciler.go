package ghostferry

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

// This event generates a REPLACE INTO SQL statement to overwrite a row if an
// INSERT or UPDATE occured. For a DELETE statement, it generates the
// corresponding DELETE statement based on the primary key.
//
// This is used during the resume when we are catching up to the binlog events
// missed by Ghostferry while it is down.
type ReconcilationDMLEvent struct {
	*DMLEventBase
	newValues RowData
	pk        uint64
}

func (e *ReconcilationDMLEvent) OldValues() RowData {
	return nil
}

func (e *ReconcilationDMLEvent) NewValues() RowData {
	return e.newValues
}

func (e *ReconcilationDMLEvent) PK() (uint64, error) {
	return e.pk, nil
}

func (e *ReconcilationDMLEvent) AsSQLString(target *schema.Table) (string, error) {
	var query string
	if e.newValues != nil {
		columns, err := loadColumnsForTable(&e.table, e.newValues)
		if err != nil {
			return "", err
		}

		query = "REPLACE INTO " +
			QuotedTableNameFromString(target.Schema, target.Name) +
			" (" + strings.Join(columns, ",") + ")" +
			" VALUES (" + buildStringListForValues(e.newValues) + ")"
	} else {
		pkColumnName := e.TableSchema().GetPKColumn(0).Name
		if pkColumnName == "" {
			return "", fmt.Errorf("cannot get PK column for table %s", e.Table())
		}

		pkColumnName = quoteField(pkColumnName)
		query = "DELETE FROM " + QuotedTableNameFromString(target.Schema, target.Name) +
			" WHERE " + buildStringMapForWhere([]string{pkColumnName}, []interface{}{e.pk})
	}

	return query, nil
}

func NewReconciliationDMLEvent(table *schema.Table, pk uint64, row RowData) DMLEvent {
	return &ReconcilationDMLEvent{
		DMLEventBase: &DMLEventBase{table: *table},
		pk:           pk,
		newValues:    row,
	}
}

// Instead of replacing/deleting every row, we first store all the rows changed
// during the downtime and perform all the operations at the end. This maybe
// faster (trading memory usage tho) and it is crucial for schema changes (as
// we can simply delete a key from this map when we realize a table has
// changed).
type UniqueRowMap map[TableIdentifier]map[uint64]struct{}

func (m UniqueRowMap) AddRow(table *schema.Table, pk uint64) bool {
	tableId := NewTableIdentifierFromSchemaTable(table)
	if _, exists := m[tableId]; !exists {
		m[tableId] = make(map[uint64]struct{})
	}

	if _, exists := m[tableId][pk]; !exists {
		m[tableId][pk] = struct{}{}
		return true
	}

	return false
}

type BinlogReconciler struct {
	BatchSize        int
	TableSchemaCache TableSchemaCache

	SourceDB     *sql.DB
	BinlogWriter *BinlogWriter

	modifiedRows UniqueRowMap
	logger       *logrus.Entry
}

func (r *BinlogReconciler) Initialize() {
	r.modifiedRows = make(UniqueRowMap)
	r.logger = logrus.WithField("tag", "binlogreconcile")
}

func (r *BinlogReconciler) AddRowsToStore(events []DMLEvent) error {
	for _, ev := range events {
		pk, err := ev.PK()
		if err != nil {
			return err
		}

		r.modifiedRows.AddRow(ev.TableSchema(), pk)
	}

	return nil
}

func (r *BinlogReconciler) ReplaceModifiedRowsAfterCatchup() error {
	batch := make([]DMLEvent, 0, r.BatchSize)

	totalModifiedRows := 0
	for _, pkSet := range r.modifiedRows {
		totalModifiedRows += len(pkSet)
	}

	r.logger.WithField("row_count", totalModifiedRows).Info("begin replacing modified rows")

	count := 0
	for tableId, pkSet := range r.modifiedRows {
		table := r.TableSchemaCache.Get(tableId.SchemaName, tableId.TableName)

		for pk, _ := range pkSet {
			count++

			if len(batch) == r.BatchSize {
				r.logger.WithField("rows_replaced", count).Debug("replacing batch")
				err := r.replaceBatch(batch)
				if err != nil {
					return err
				}

				batch = make([]DMLEvent, 0, r.BatchSize)
			}

			row, err := r.selectRowFromSource(table, pk)
			if err != nil {
				r.logger.WithError(err).Error("failed to select row from source")
				return err
			}

			batch = append(batch, NewReconciliationDMLEvent(table, pk, row))
		}
	}

	if len(batch) > 0 {
		err := r.replaceBatch(batch)
		if err != nil {
			return err
		}
	}

	r.logger.WithField("row_count", totalModifiedRows).Info("replaced modified rows")

	return nil
}

func (r *BinlogReconciler) replaceBatch(batch []DMLEvent) error {
	err := r.BinlogWriter.WriteEvents(batch)
	if err != nil {
		r.logger.WithError(err).Error("cannot replace batch")
		return err
	}

	return nil
}

func (r *BinlogReconciler) selectRowFromSource(table *schema.Table, pk uint64) (RowData, error) {
	quotedPK := quoteField(table.GetPKColumn(0).Name)

	query, args, err := squirrel.
		Select("*").
		From(QuotedTableName(table)).
		Where(squirrel.Eq{quotedPK: pk}).ToSql()

	if err != nil {
		return nil, err
	}

	// TODO: make this cached for faster reconciliation
	stmt, err := r.SourceDB.Prepare(query)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rowData RowData = nil
	count := 0

	for rows.Next() {
		rowData, err = ScanGenericRow(rows, len(table.Columns))
		if err != nil {
			return nil, err
		}

		count++
	}

	if count > 1 {
		return nil, fmt.Errorf("multiple rows detected when only one or zero is expected for %s %v", table.String(), pk)
	}

	return rowData, rows.Err()
}
