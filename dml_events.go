package ghostferry

import (
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

type DMLEvent interface {
	Database() string
	Table() string
	TableSchema() *schema.Table
	AsSQLQuery(target *schema.Table) (string, []interface{}, error)
	OldValues() []interface{}
	NewValues() []interface{}
}

type DMLEventBase struct {
	table schema.Table
}

func (e *DMLEventBase) Database() string {
	return e.table.Schema
}

func (e *DMLEventBase) Table() string {
	return e.table.Name
}

func (e *DMLEventBase) TableSchema() *schema.Table {
	return &e.table
}

type BinlogInsertEvent struct {
	newValues []interface{}
	DMLEventBase
}

func NewBinlogInsertEvents(table *schema.Table, rowsEvent *replication.RowsEvent) ([]DMLEvent, error) {
	insertEvents := make([]DMLEvent, len(rowsEvent.Rows))

	for i, row := range rowsEvent.Rows {
		insertEvents[i] = &BinlogInsertEvent{
			newValues:    row,
			DMLEventBase: DMLEventBase{table: *table},
		}
	}

	return insertEvents, nil
}

func (e *BinlogInsertEvent) OldValues() []interface{} {
	return nil
}

func (e *BinlogInsertEvent) NewValues() []interface{} {
	return e.newValues
}

func (e *BinlogInsertEvent) AsSQLQuery(target *schema.Table) (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(&e.table, e.newValues)
	if err != nil {
		return "", nil, err
	}

	query := "INSERT IGNORE INTO " + QuotedTableNameFromString(target.Schema, target.Name) +
		" (" + strings.Join(columns, ",") + ") VALUES (" + strings.Repeat("?,", len(columns)-1) + "?)"

	return query, e.newValues, nil
}

type BinlogUpdateEvent struct {
	oldValues []interface{}
	newValues []interface{}
	DMLEventBase
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
			oldValues:    row,
			newValues:    rowsEvent.Rows[i+1],
			DMLEventBase: DMLEventBase{table: *table},
		}
	}

	return updateEvents, nil
}

func (e *BinlogUpdateEvent) OldValues() []interface{} {
	return e.oldValues
}

func (e *BinlogUpdateEvent) NewValues() []interface{} {
	return e.newValues
}

func (e *BinlogUpdateEvent) AsSQLQuery(target *schema.Table) (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(&e.table, e.oldValues, e.newValues)
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
	oldValues []interface{}
	DMLEventBase
}

func (e *BinlogDeleteEvent) OldValues() []interface{} {
	return e.oldValues
}

func (e *BinlogDeleteEvent) NewValues() []interface{} {
	return nil
}

func NewBinlogDeleteEvents(table *schema.Table, rowsEvent *replication.RowsEvent) ([]DMLEvent, error) {
	deleteEvents := make([]DMLEvent, len(rowsEvent.Rows))

	for i, row := range rowsEvent.Rows {
		deleteEvents[i] = &BinlogDeleteEvent{
			oldValues:    row,
			DMLEventBase: DMLEventBase{table: *table},
		}
	}

	return deleteEvents, nil
}

func (e *BinlogDeleteEvent) AsSQLQuery(target *schema.Table) (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(&e.table, e.oldValues)
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

type ExistingRowEvent struct {
	values []interface{}
	DMLEventBase
}

func NewExistingRowEvent(table *schema.Table, values []interface{}) (DMLEvent, error) {
	return &ExistingRowEvent{
		values:       values,
		DMLEventBase: DMLEventBase{table: *table},
	}, nil
}

func (e *ExistingRowEvent) OldValues() []interface{} {
	return e.values
}

func (e *ExistingRowEvent) NewValues() []interface{} {
	return e.values
}

func (e *ExistingRowEvent) AsSQLQuery(target *schema.Table) (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(&e.table, e.values)
	if err != nil {
		return "", nil, err
	}

	query := "INSERT IGNORE INTO " + QuotedTableNameFromString(target.Schema, target.Name) +
		" (" + strings.Join(columns, ",") + ") VALUES (" + strings.Repeat("?,", len(columns)-1) + "?)"

	return query, e.values, nil
}

func conditionsForTable(columns []string, args []interface{}) ([]string, []interface{}) {
	conditions := make([]string, len(columns))
	values := make([]interface{}, 0, len(args))

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

func loadColumnsForEvent(table *schema.Table, valuesToVerify ...[]interface{}) ([]string, error) {
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

func verifyValuesHasTheSameLengthAsColumns(table *schema.Table, values []interface{}) error {
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
