package ghostferry

import (
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

type RowData []interface{}

type DMLEvent interface {
	Database() string
	Table() string
	TableSchema() *schema.Table
	AsSQLQuery(target *schema.Table) (string, []interface{}, error)
	OldValues() RowData
	NewValues() RowData
}

type BinlogInsertEvent struct {
	newValues RowData
	TableCopy
}

func NewBinlogInsertEvents(table *schema.Table, rowsEvent *replication.RowsEvent) ([]DMLEvent, error) {
	insertEvents := make([]DMLEvent, len(rowsEvent.Rows))

	for i, row := range rowsEvent.Rows {
		insertEvents[i] = &BinlogInsertEvent{
			newValues: row,
			TableCopy: TableCopy{table: *table},
		}
	}

	return insertEvents, nil
}

func (e *BinlogInsertEvent) OldValues() RowData {
	return nil
}

func (e *BinlogInsertEvent) NewValues() RowData {
	return e.newValues
}

func (e *BinlogInsertEvent) AsSQLQuery(target *schema.Table) (string, []interface{}, error) {
	columns, err := loadColumnsForTable(&e.table, e.newValues)
	if err != nil {
		return "", nil, err
	}

	query := "INSERT IGNORE INTO " + QuotedTableNameFromString(target.Schema, target.Name) +
		" (" + strings.Join(columns, ",") + ") VALUES (" + strings.Repeat("?,", len(columns)-1) + "?)"

	return query, e.newValues, nil
}

type BinlogUpdateEvent struct {
	oldValues RowData
	newValues RowData
	TableCopy
}

func NewBinlogUpdateEvents(table *schema.Table, rowsEvent *replication.RowsEvent) ([]DMLEvent, error) {
	// UPDATE events have two rows in the RowsEvent. The first row is the
	// entries of the old record (for WHERE) and the second row is the
	// entries of the new record (for SET).
	// There can be n db rows changed in one RowsEvent, resulting in
	// 2*n binlog rows.
	updateEvents := make([]DMLEvent, len(rowsEvent.Rows)/2)

	for i, row := range rowsEvent.Rows {
		if i%2 == 1 {
			continue
		}

		updateEvents[i/2] = &BinlogUpdateEvent{
			oldValues: row,
			newValues: rowsEvent.Rows[i+1],
			TableCopy: TableCopy{table: *table},
		}
	}

	return updateEvents, nil
}

func (e *BinlogUpdateEvent) OldValues() RowData {
	return e.oldValues
}

func (e *BinlogUpdateEvent) NewValues() RowData {
	return e.newValues
}

func (e *BinlogUpdateEvent) AsSQLQuery(target *schema.Table) (string, []interface{}, error) {
	columns, err := loadColumnsForTable(&e.table, e.oldValues, e.newValues)
	if err != nil {
		return "", nil, err
	}

	setConditions, _ := conditionsForTable(columns, nil)
	whereConditions, whereValues := conditionsForTable(columns, e.oldValues)

	query := "UPDATE " + QuotedTableNameFromString(target.Schema, target.Name) +
		" SET " + strings.Join(setConditions, ", ") +
		" WHERE " + strings.Join(whereConditions, " AND ")

	var args []interface{}
	args = append(args, e.newValues...)
	args = append(args, whereValues...)
	return query, args, nil
}

type BinlogDeleteEvent struct {
	oldValues RowData
	TableCopy
}

func (e *BinlogDeleteEvent) OldValues() RowData {
	return e.oldValues
}

func (e *BinlogDeleteEvent) NewValues() RowData {
	return nil
}

func NewBinlogDeleteEvents(table *schema.Table, rowsEvent *replication.RowsEvent) ([]DMLEvent, error) {
	deleteEvents := make([]DMLEvent, len(rowsEvent.Rows))

	for i, row := range rowsEvent.Rows {
		deleteEvents[i] = &BinlogDeleteEvent{
			oldValues: row,
			TableCopy: TableCopy{table: *table},
		}
	}

	return deleteEvents, nil
}

func (e *BinlogDeleteEvent) AsSQLQuery(target *schema.Table) (string, []interface{}, error) {
	columns, err := loadColumnsForTable(&e.table, e.oldValues)
	if err != nil {
		return "", nil, err
	}

	whereConditions, whereValues := conditionsForTable(columns, e.oldValues)

	query := "DELETE FROM " + QuotedTableNameFromString(target.Schema, target.Name) +
		" WHERE " + strings.Join(whereConditions, " AND ")

	return query, whereValues, nil
}

func NewBinlogDMLEvents(table *schema.Table, ev *replication.BinlogEvent) ([]DMLEvent, error) {
	rowsEvent := ev.Event.(*replication.RowsEvent)

	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return NewBinlogInsertEvents(table, rowsEvent)
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return NewBinlogDeleteEvents(table, rowsEvent)
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return NewBinlogUpdateEvents(table, rowsEvent)
	default:
		return nil, fmt.Errorf("unrecognized rows event: %s", ev.Header.EventType.String())
	}
}

func conditionsForTable(columns []string, args RowData) ([]string, RowData) {
	conditions := make([]string, len(columns))
	values := make(RowData, 0, len(args))

	for i, name := range columns {
		if args != nil && args[i] == nil {
			conditions[i] = name + " IS NULL"
		} else {
			conditions[i] = name + " = ?"

			if args != nil {
				values = append(values, args[i])
			}
		}
	}

	return conditions, values
}

func loadColumnsForTable(table *schema.Table, valuesToVerify ...RowData) ([]string, error) {
	for _, values := range valuesToVerify {
		if err := verifyValuesHasTheSameLengthAsColumns(table, values); err != nil {
			return nil, err
		}
	}

	return quotedColumnNames(table), nil
}

func quotedColumnNames(table *schema.Table) []string {
	cols := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		cols[i] = quoteField(column.Name)
	}
	return cols
}

func verifyValuesHasTheSameLengthAsColumns(table *schema.Table, values RowData) error {
	if len(table.Columns) != len(values) {
		return fmt.Errorf(
			"table %s.%s has %d columns but event has %d columns instead",
			table.Schema,
			table.Name,
			len(table.Columns),
			len(values),
		)
	}
	return nil
}
