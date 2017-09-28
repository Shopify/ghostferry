package ghostferry

import (
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/replication"
)

type DMLEvent interface {
	Database() string
	Table() string
	AsSQLQuery(TableSchemaCache) (string, []interface{}, error)
	OldValues() []interface{}
	NewValues() []interface{}
}

type BinlogInsertEvent struct {
	database  string
	table     string
	newValues []interface{}
}

func NewBinlogInsertEvents(rowsEvent *replication.RowsEvent) []DMLEvent {
	database := string(rowsEvent.Table.Schema)
	table := string(rowsEvent.Table.Table)
	insertEvents := make([]DMLEvent, len(rowsEvent.Rows))

	for i, row := range rowsEvent.Rows {
		insertEvents[i] = &BinlogInsertEvent{
			database:  database,
			table:     table,
			newValues: row,
		}
	}

	return insertEvents
}

func (e *BinlogInsertEvent) Database() string {
	return e.database
}

func (e *BinlogInsertEvent) Table() string {
	return e.table
}

func (e *BinlogInsertEvent) OldValues() []interface{} {
	return make([]interface{}, len(e.newValues))
}

func (e *BinlogInsertEvent) NewValues() []interface{} {
	return e.newValues
}

func (e *BinlogInsertEvent) AsSQLQuery(tables TableSchemaCache) (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(tables, e.database, e.table, e.newValues)
	if err != nil {
		return "", []interface{}{}, err
	}

	query := "INSERT IGNORE INTO " + QuotedTableNameFromString(e.database, e.table) +
		" (" + strings.Join(columns, ",") + ") VALUES (" + strings.Repeat("?,", len(columns)-1) + "?)"

	return query, e.newValues, nil
}

type BinlogUpdateEvent struct {
	database    string
	table       string
	whereValues []interface{}
	newValues   []interface{}
}

func NewBinlogUpdateEvents(rowsEvent *replication.RowsEvent) []DMLEvent {
	database := string(rowsEvent.Table.Schema)
	table := string(rowsEvent.Table.Table)

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
			database:    database,
			table:       table,
			whereValues: row,
			newValues:   rowsEvent.Rows[i+1],
		}
	}

	return updateEvents
}

func (e *BinlogUpdateEvent) Database() string {
	return e.database
}

func (e *BinlogUpdateEvent) Table() string {
	return e.table
}

func (e *BinlogUpdateEvent) OldValues() []interface{} {
	return e.whereValues
}

func (e *BinlogUpdateEvent) NewValues() []interface{} {
	return e.newValues
}

func (e *BinlogUpdateEvent) AsSQLQuery(tables TableSchemaCache) (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(tables, e.database, e.table, e.whereValues, e.newValues)
	if err != nil {
		return "", []interface{}{}, err
	}

	conditions := conditionsForTable(columns)

	query := "UPDATE " + QuotedTableNameFromString(e.database, e.table) +
		" SET " + strings.Join(conditions, ", ") +
		" WHERE " + strings.Join(conditions, " AND ")

	var args []interface{}
	args = append(args, e.newValues...)
	args = append(args, e.whereValues...)
	return query, args, nil
}

type BinlogDeleteEvent struct {
	database    string
	table       string
	whereValues []interface{}
}

func (e *BinlogDeleteEvent) OldValues() []interface{} {
	return e.whereValues
}

func (e *BinlogDeleteEvent) NewValues() []interface{} {
	return make([]interface{}, len(e.whereValues))
}

func NewBinlogDeleteEvents(rowsEvent *replication.RowsEvent) []DMLEvent {
	database := string(rowsEvent.Table.Schema)
	table := string(rowsEvent.Table.Table)
	deleteEvents := make([]DMLEvent, len(rowsEvent.Rows))

	for i, row := range rowsEvent.Rows {
		deleteEvents[i] = &BinlogDeleteEvent{
			database:    database,
			table:       table,
			whereValues: row,
		}
	}

	return deleteEvents
}

func (e *BinlogDeleteEvent) Database() string {
	return e.database
}

func (e *BinlogDeleteEvent) Table() string {
	return e.table
}

func (e *BinlogDeleteEvent) AsSQLQuery(tables TableSchemaCache) (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(tables, e.database, e.table, e.whereValues)
	if err != nil {
		return "", []interface{}{}, err
	}

	query := "DELETE FROM " + QuotedTableNameFromString(e.database, e.table) +
		" WHERE " + strings.Join(conditionsForTable(columns), " AND ")

	return query, e.whereValues, nil
}

func NewBinlogDMLEvents(ev *replication.BinlogEvent) ([]DMLEvent, error) {
	rowsEvent := ev.Event.(*replication.RowsEvent)
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return NewBinlogInsertEvents(rowsEvent), nil
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return NewBinlogDeleteEvents(rowsEvent), nil
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return NewBinlogUpdateEvents(rowsEvent), nil
	default:
		return []DMLEvent{}, fmt.Errorf("unrecognized rows event: %s", ev.Header.EventType.String())
	}
}

type ExistingRowEvent struct {
	database string
	table    string
	values   []interface{}
}

func NewExistingRowEvent(database, table string, values []interface{}) DMLEvent {
	return &ExistingRowEvent{
		database: database,
		table:    table,
		values:   values,
	}
}

func (e *ExistingRowEvent) Database() string {
	return e.database
}

func (e *ExistingRowEvent) Table() string {
	return e.table
}

func (e *ExistingRowEvent) OldValues() []interface{} {
	return e.values
}

func (e *ExistingRowEvent) NewValues() []interface{} {
	return e.values
}

func (e *ExistingRowEvent) AsSQLQuery(tables TableSchemaCache) (string, []interface{}, error) {
	columns, err := loadColumnsForEvent(tables, e.database, e.table, e.values)
	if err != nil {
		return "", []interface{}{}, err
	}

	query := "INSERT IGNORE INTO " + QuotedTableNameFromString(e.database, e.table) +
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

func loadColumnsForEvent(tables TableSchemaCache, database, table string, valuesToVerify ...[]interface{}) ([]string, error) {
	columns, err := tables.TableColumnNamesQuoted(database, table)
	if err != nil {
		return nil, err
	}

	for _, values := range valuesToVerify {
		if err := verifyValuesHasTheSameLengthAsColumns(columns, values, database, table); err != nil {
			return nil, err
		}
	}

	return columns, nil
}

func verifyValuesHasTheSameLengthAsColumns(tableColumns []string, values []interface{}, databaseHint, tableHint string) error {
	if len(tableColumns) != len(values) {
		return fmt.Errorf(
			"table %s.%s has %d columns but event has %d columns instead",
			databaseHint,
			tableHint,
			len(tableColumns),
			len(values),
		)
	}
	return nil
}
