package ghostferry

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

type RowData []interface{}

type DMLEvent interface {
	Database() string
	Table() string
	TableSchema() *schema.Table
	AsSQLString(target *schema.Table) (string, error)
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

func (e *BinlogInsertEvent) AsSQLString(target *schema.Table) (string, error) {
	columns, err := loadColumnsForTable(&e.table, e.newValues)
	if err != nil {
		return "", err
	}

	query := "INSERT IGNORE INTO " +
		QuotedTableNameFromString(target.Schema, target.Name) +
		" (" + strings.Join(columns, ",") + ")" +
		" VALUES (" + buildStringListForValues(e.newValues) + ")"

	return query, nil
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

func (e *BinlogUpdateEvent) AsSQLString(target *schema.Table) (string, error) {
	columns, err := loadColumnsForTable(&e.table, e.oldValues, e.newValues)
	if err != nil {
		return "", err
	}

	query := "UPDATE " + QuotedTableNameFromString(target.Schema, target.Name) +
		" SET " + buildStringMapForSet(columns, e.newValues) +
		" WHERE " + buildStringMapForWhere(columns, e.oldValues)

	return query, nil
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

func (e *BinlogDeleteEvent) AsSQLString(target *schema.Table) (string, error) {
	columns, err := loadColumnsForTable(&e.table, e.oldValues)
	if err != nil {
		return "", err
	}

	query := "DELETE FROM " + QuotedTableNameFromString(target.Schema, target.Name) +
		" WHERE " + buildStringMapForWhere(columns, e.oldValues)

	return query, nil
}

func NewBinlogDMLEvents(table *schema.Table, ev *replication.BinlogEvent) ([]DMLEvent, error) {
	rowsEvent := ev.Event.(*replication.RowsEvent)

	for _, row := range rowsEvent.Rows {
		if len(row) != len(table.Columns) {
			return nil, fmt.Errorf(
				"table %s.%s has %d columns but event has %d columns instead",
				table.Schema,
				table.Name,
				len(table.Columns),
				len(row),
			)
		}
		for i, col := range table.Columns {
			if col.IsUnsigned {
				switch v := row[i].(type) {
				case int64:
					row[i] = uint64(v)
				case int32:
					row[i] = uint32(v)
				case int16:
					row[i] = uint16(v)
				case int8:
					row[i] = uint8(v)
				case int:
					row[i] = uint(v)
				}
			}
		}
	}

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

func buildStringListForValues(values []interface{}) string {
	var buffer []byte

	for i, value := range values {
		if i > 0 {
			buffer = append(buffer, ',')
		}

		buffer = appendEscapedValue(buffer, value)
	}

	return string(buffer)
}

func buildStringMapForWhere(columns []string, values []interface{}) string {
	var buffer []byte

	for i, value := range values {
		if i > 0 {
			buffer = append(buffer, " AND "...)
		}

		buffer = append(buffer, columns[i]...)

		if isNilValue(value) {
			// "WHERE value = NULL" will never match rows.
			buffer = append(buffer, " IS NULL"...)
		} else {
			buffer = append(buffer, '=')
			buffer = appendEscapedValue(buffer, value)
		}
	}

	return string(buffer)
}

func buildStringMapForSet(columns []string, values []interface{}) string {
	var buffer []byte

	for i, value := range values {
		if i > 0 {
			buffer = append(buffer, ',')
		}

		buffer = append(buffer, columns[i]...)
		buffer = append(buffer, '=')
		buffer = appendEscapedValue(buffer, value)
	}

	return string(buffer)
}

func isNilValue(value interface{}) bool {
	if value == nil {
		return true
	} else if vb, ok := value.([]byte); ok && vb == nil {
		return true
	}
	return false
}

func appendEscapedValue(buffer []byte, value interface{}) []byte {
	if isNilValue(value) {
		return append(buffer, "NULL"...)
	}

	switch v := value.(type) {
	case string:
		return appendEscapedString(buffer, v)
	case []byte:
		return appendEscapedBuffer(buffer, v)
	case uint64:
		return strconv.AppendUint(buffer, v, 10)
	case int64:
		return strconv.AppendInt(buffer, v, 10)
	case uint32:
		return strconv.AppendUint(buffer, uint64(v), 10)
	case int32:
		return strconv.AppendInt(buffer, int64(v), 10)
	case uint16:
		return strconv.AppendUint(buffer, uint64(v), 10)
	case int16:
		return strconv.AppendInt(buffer, int64(v), 10)
	case uint8:
		return strconv.AppendUint(buffer, uint64(v), 10)
	case int8:
		return strconv.AppendInt(buffer, int64(v), 10)
	case int:
		return strconv.AppendInt(buffer, int64(v), 10)
	case bool:
		if v {
			return append(buffer, '1')
		} else {
			return append(buffer, '0')
		}
	case float64:
		return strconv.AppendFloat(buffer, v, 'g', -1, 64)
	case float32:
		return strconv.AppendFloat(buffer, float64(v), 'g', -1, 64)
	default:
		panic(fmt.Sprintf("unsupported type %t", value))
	}
}

// appendEscapedString replaces single quotes with quote-escaped single quotes.
// When the NO_BACKSLASH_ESCAPES mode is on, this is the extent of escaping
// necessary for strings.
//
// ref: https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L963-L1038
// ref: https://github.com/go-sql-driver/mysql/blob/9181e3a86a19bacd63e68d43ae8b7b36320d8092/utils.go#L717-L758
func appendEscapedString(buffer []byte, value string) []byte {
	buffer = append(buffer, '\'')

	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '\'' {
			buffer = append(buffer, '\'', '\'')
		} else {
			buffer = append(buffer, c)
		}
	}

	return append(buffer, '\'')
}

func appendEscapedBuffer(buffer, value []byte) []byte {
	buffer = append(buffer, "_binary'"...)

	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '\'' {
			buffer = append(buffer, '\'', '\'')
		} else {
			buffer = append(buffer, c)
		}
	}

	return append(buffer, '\'')
}
