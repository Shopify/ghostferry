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
	Schema() schema.Table
	AsSQLQuery() (string, []interface{}, error)
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

func (e *DMLEventBase) Schema() schema.Table {
	return e.table
}

type BinlogInsertEvent struct {
	newValues []interface{}
	DMLEventBase
}

func NewBinlogInsertEvents(rowsEvent *replication.RowsEvent, tables TableSchemaCache) ([]DMLEvent, error) {
	table, err := tables.Get(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
	if err != nil {
		return nil, err
	}

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

func (e *BinlogInsertEvent) AsSQLQuery() (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(e.table, e.newValues)
	if err != nil {
		return "", nil, err
	}

	query := "INSERT IGNORE INTO " + QuotedTableNameFromString(e.table.Schema, e.table.Name) +
		" (" + strings.Join(columns, ",") + ") VALUES (" + strings.Repeat("?,", len(columns)-1) + "?)"

	return query, e.newValues, nil
}

type BinlogUpdateEvent struct {
	oldValues []interface{}
	newValues []interface{}
	DMLEventBase
}

func NewBinlogUpdateEvents(rowsEvent *replication.RowsEvent, tables TableSchemaCache) ([]DMLEvent, error) {
	table, err := tables.Get(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
	if err != nil {
		return nil, err
	}

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

func (e *BinlogUpdateEvent) AsSQLQuery() (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(e.table, e.oldValues, e.newValues)
	if err != nil {
		return "", nil, err
	}

	conditions := conditionsForTable(columns)

	query := "UPDATE " + QuotedTableNameFromString(e.table.Schema, e.table.Name) +
		" SET " + strings.Join(conditions, ", ") +
		" WHERE " + strings.Join(conditions, " AND ")

	var args []interface{}
	args = append(args, e.newValues...)
	args = append(args, e.oldValues...)
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

func NewBinlogDeleteEvents(rowsEvent *replication.RowsEvent, tables TableSchemaCache) ([]DMLEvent, error) {
	table, err := tables.Get(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
	if err != nil {
		return nil, err
	}

	deleteEvents := make([]DMLEvent, len(rowsEvent.Rows))

	for i, row := range rowsEvent.Rows {
		deleteEvents[i] = &BinlogDeleteEvent{
			oldValues:    row,
			DMLEventBase: DMLEventBase{table: *table},
		}
	}

	return deleteEvents, nil
}

func (e *BinlogDeleteEvent) AsSQLQuery() (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(e.table, e.oldValues)
	if err != nil {
		return "", nil, err
	}

	query := "DELETE FROM " + QuotedTableNameFromString(e.table.Schema, e.table.Name) +
		" WHERE " + strings.Join(conditionsForTable(columns), " AND ")

	return query, e.oldValues, nil
}

func NewBinlogDMLEvents(ev *replication.BinlogEvent, tables TableSchemaCache) ([]DMLEvent, error) {
	rowsEvent := ev.Event.(*replication.RowsEvent)
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return NewBinlogInsertEvents(rowsEvent, tables)
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return NewBinlogDeleteEvents(rowsEvent, tables)
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return NewBinlogUpdateEvents(rowsEvent, tables)
	default:
		return []DMLEvent{}, fmt.Errorf("unrecognized rows event: %s", ev.Header.EventType.String())
	}
}

type ExistingRowEvent struct {
	values []interface{}
	DMLEventBase
}

func NewExistingRowEvent(databaseName, tableName string, values []interface{}, tables TableSchemaCache) (DMLEvent, error) {
	table, err := tables.Get(databaseName, tableName)
	if err != nil {
		return nil, err
	}

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

func (e *ExistingRowEvent) AsSQLQuery() (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(e.table, e.values)
	if err != nil {
		return "", nil, err
	}

	query := "INSERT IGNORE INTO " + QuotedTableNameFromString(e.table.Schema, e.table.Name) +
		" (" + strings.Join(columns, ",") + ") VALUES (" + strings.Repeat("?,", len(columns)-1) + "?)"

	return query, e.values, nil
}

func conditionsForTable(columns []string) []string {
	conditions := make([]string, len(columns))
	for i, name := range columns {
		conditions[i] = name + " = ?"
	}
	return conditions
}

func loadColumnsForEvent(table schema.Table, valuesToVerify ...[]interface{}) ([]string, error) {
	for _, values := range valuesToVerify {
		if err := verifyValuesHasTheSameLengthAsColumns(table, values); err != nil {
			return nil, err
		}
	}

	return quoteColumnNames(table), nil
}

func quoteColumnNames(table schema.Table) []string {
	cols := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		cols[i] = quoteField(column.Name)
	}
	return cols
}

func verifyValuesHasTheSameLengthAsColumns(table schema.Table, values []interface{}) error {
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
