package ghostferry

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/siddontang/go-mysql/replication"
)

func verifyValuesHasTheSameLengthAsColumns(tableColumns []string, values []interface{}, databaseHint, tableHint string) error {
	if len(tableColumns) != len(values) {
		return fmt.Errorf(
			"table %s.%s has %d columns but binlog has %d columns instead",
			databaseHint,
			tableHint,
			len(tableColumns),
			len(values),
		)
	}
	return nil
}

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
	tableColumns, err := tables.TableColumnNames(e.database, e.table)
	if err != nil {
		return "", []interface{}{}, err
	}

	err = verifyValuesHasTheSameLengthAsColumns(tableColumns, e.newValues, e.database, e.table)
	if err != nil {
		return "", []interface{}{}, err
	}

	// temporarily use a sql builder because otherwise it will take a long time
	return sq.Insert(fmt.Sprintf("%s.%s", e.database, e.table)).
		Options("IGNORE").
		Columns(tableColumns...).
		Values(e.newValues...).ToSql()
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
	tableColumns, err := tables.TableColumnNames(e.database, e.table)
	if err != nil {
		return "", []interface{}{}, err
	}

	err = verifyValuesHasTheSameLengthAsColumns(tableColumns, e.newValues, e.database, e.table)
	if err != nil {
		return "", []interface{}{}, err
	}

	err = verifyValuesHasTheSameLengthAsColumns(tableColumns, e.whereValues, e.database, e.table)
	if err != nil {
		return "", []interface{}{}, err
	}

	setArgs := make(map[string]interface{})
	whereArgs := make(map[string]interface{})
	for i, column := range tableColumns {
		setArgs[column] = e.newValues[i]
		whereArgs[column] = e.whereValues[i]
	}

	// temporarily use a sql builder because otherwise it will take a long time
	return sq.Update(fmt.Sprintf("%s.%s", e.database, e.table)).
		SetMap(setArgs).
		Where(whereArgs).ToSql()
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
	tableColumns, err := tables.TableColumnNames(e.database, e.table)
	if err != nil {
		return "", []interface{}{}, err
	}

	err = verifyValuesHasTheSameLengthAsColumns(tableColumns, e.whereValues, e.database, e.table)
	if err != nil {
		return "", []interface{}{}, err
	}

	whereArgs := make(map[string]interface{})
	for i, column := range tableColumns {
		whereArgs[column] = e.whereValues[i]
	}

	// temporarily use a sql builder because otherwise it will take a long time
	return sq.Delete(fmt.Sprintf("%s.%s", e.database, e.table)).
		Where(whereArgs).ToSql()
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
	tableColumns, err := tables.TableColumnNames(e.database, e.table)
	if err != nil {
		return "", []interface{}{}, err
	}

	err = verifyValuesHasTheSameLengthAsColumns(tableColumns, e.values, e.database, e.table)
	if err != nil {
		return "", []interface{}{}, err
	}

	return sq.Insert(fmt.Sprintf("%s.%s", e.database, e.table)).
		Options("IGNORE").
		Columns(tableColumns...).
		Values(e.values...).ToSql()
}

type DMLEventFilter interface {
	Applicable(DMLEvent) bool
}

type NoFilter struct{}

func NewNoFilter() *NoFilter {
	return &NoFilter{}
}

func (f *NoFilter) Applicable(ev DMLEvent) bool {
	return true
}
