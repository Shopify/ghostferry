package ghostferry

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

var annotationRegex = regexp.MustCompile(`^/\*(.*?)\*/`)

type RowData []interface{}

// For historical reasons, this function ended up being used at two different
// places: the DataIterator and the DMLEvent (which indirectly is used by the
// BinlogStreamer, InlineVerifier, etc).
//
// The original code here was introduced in
// 152caec0ff5195d4698672df6dc0f72fb77b02fc, where it is used solely in context
// of the DataIterator. In this context, the value being parsed here is given to
// us by the go-sql-driver/mysql driver. This value could either by int64 or it
// could be a byte slice decimal string with the uint64 value in it, which is
// why we have this awkward byte slice to integer trick. This also means the
// original code is not designed to handle uint64, as go-sql-driver/mysql never
// returns uint64. This could possibly be an upstream problem that have since
// been fixed, but this was not investigated.
// (A possibly related PR: https://github.com/go-sql-driver/mysql/pull/690)
//
// At some point, this code was refactored into this function, such that the
// BinlogStreamer also uses the same code to decode integers. The binlog data is
// given to us by siddontang/go-mysql. The siddontang/go-mysql library should
// not be giving us awkward byte slices. Instead, it should properly gives us
// uint64. This code thus panics when it encounters such case. See
// https://github.com/Shopify/ghostferry/issues/165.
//
// In summary:
// - This code receives values from both go-sql-driver/mysql and
//   siddontang/go-mysql.
// - go-sql-driver/mysql gives us int64 for signed integer, and uint64 in a byte
//   slice for unsigned integer.
// - siddontang/go-mysql gives us int64 for signed integer, and uint64 for
//   unsigned integer.
// - We currently make this function deal with both cases. In the future we can
//   investigate alternative solutions.
func (r RowData) GetUint64(colIdx int) (uint64, error) {
	u64, ok := Uint64Value(r[colIdx])
	if ok {
		return u64, nil
	}

	i64, ok := Int64Value(r[colIdx])
	if ok {
		return uint64(i64), nil
	}

	if valueByteSlice, ok := r[colIdx].([]byte); ok {
		valueString := string(valueByteSlice)
		return strconv.ParseUint(valueString, 10, 64)
	}

	return 0, fmt.Errorf("expected position %d to contain integer, but got type %T instead (with value of %v)", colIdx, r[colIdx], r[colIdx])
}

type DMLEvent interface {
	Database() string
	Table() string
	TableSchema() *TableSchema
	AsSQLString(string, string) (string, error)
	OldValues() RowData
	NewValues() RowData
	PaginationKey() (uint64, error)
	BinlogPosition() mysql.Position
	ResumableBinlogPosition() mysql.Position
	Annotation() (string, error)
	Timestamp() time.Time
}

// The base of DMLEvent to provide the necessary methods.
type DMLEventBase struct {
	table        *TableSchema
	pos          mysql.Position
	resumablePos mysql.Position
	query        []byte
	timestamp    time.Time
}

func (e *DMLEventBase) Database() string {
	return e.table.Schema
}

func (e *DMLEventBase) Table() string {
	return e.table.Name
}

func (e *DMLEventBase) TableSchema() *TableSchema {
	return e.table
}

func (e *DMLEventBase) BinlogPosition() mysql.Position {
	return e.pos
}

func (e *DMLEventBase) ResumableBinlogPosition() mysql.Position {
	return e.resumablePos
}

// Annotation will return the first prefixed comment on the SQL string,
// or an error if the query attribute of the DMLEvent is not set
func (e *DMLEventBase) Annotation() (string, error) {
	if e.query == nil {
		return "", errors.New("could not get query from DML event")
	}

	captured := annotationRegex.FindSubmatch(e.query)
	if len(captured) > 1 {
		return string(captured[1]), nil
	}

	return "", nil
}

func (e *DMLEventBase) Timestamp() time.Time {
	return e.timestamp
}

func NewDMLEventBase(table *TableSchema, pos, resumablePos mysql.Position, query []byte, timestamp time.Time) *DMLEventBase {
	return &DMLEventBase{
		table:        table,
		pos:          pos,
		resumablePos: resumablePos,
		query:        query,
		timestamp:    timestamp,
	}
}

type BinlogInsertEvent struct {
	newValues RowData
	*DMLEventBase
}

func NewBinlogInsertEvents(eventBase *DMLEventBase, rowsEvent *replication.RowsEvent) ([]DMLEvent, error) {
	insertEvents := make([]DMLEvent, len(rowsEvent.Rows))

	for i, row := range rowsEvent.Rows {
		insertEvents[i] = &BinlogInsertEvent{
			newValues:    row,
			DMLEventBase: eventBase,
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

func (e *BinlogInsertEvent) AsSQLString(schemaName, tableName string) (string, error) {
	if err := verifyValuesHasTheSameLengthAsColumns(e.table, e.newValues); err != nil {
		return "", err
	}

	query := "INSERT IGNORE INTO " +
		QuotedTableNameFromString(schemaName, tableName) +
		" (" + strings.Join(quotedColumnNames(e.table), ",") + ")" +
		" VALUES (" + buildStringListForValues(e.table.Columns, e.newValues) + ")"

	return query, nil
}

func (e *BinlogInsertEvent) PaginationKey() (uint64, error) {
	return paginationKeyFromEventData(e.table, e.newValues)
}

type BinlogUpdateEvent struct {
	oldValues RowData
	newValues RowData
	*DMLEventBase
}

func NewBinlogUpdateEvents(eventBase *DMLEventBase, rowsEvent *replication.RowsEvent) ([]DMLEvent, error) {
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
			DMLEventBase: eventBase,
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

func (e *BinlogUpdateEvent) AsSQLString(schemaName, tableName string) (string, error) {
	if err := verifyValuesHasTheSameLengthAsColumns(e.table, e.oldValues, e.newValues); err != nil {
		return "", err
	}

	query := "UPDATE " + QuotedTableNameFromString(schemaName, tableName) +
		" SET " + buildStringMapForSet(e.table.Columns, e.newValues) +
		" WHERE " + buildStringMapForWhere(e.table.Columns, e.oldValues)

	return query, nil
}

func (e *BinlogUpdateEvent) PaginationKey() (uint64, error) {
	return paginationKeyFromEventData(e.table, e.newValues)
}

type BinlogDeleteEvent struct {
	oldValues RowData
	*DMLEventBase
}

func (e *BinlogDeleteEvent) OldValues() RowData {
	return e.oldValues
}

func (e *BinlogDeleteEvent) NewValues() RowData {
	return nil
}

func NewBinlogDeleteEvents(eventBase *DMLEventBase, rowsEvent *replication.RowsEvent) ([]DMLEvent, error) {
	deleteEvents := make([]DMLEvent, len(rowsEvent.Rows))

	for i, row := range rowsEvent.Rows {
		deleteEvents[i] = &BinlogDeleteEvent{
			oldValues:    row,
			DMLEventBase: eventBase,
		}
	}

	return deleteEvents, nil
}

func (e *BinlogDeleteEvent) AsSQLString(schemaName, tableName string) (string, error) {
	if err := verifyValuesHasTheSameLengthAsColumns(e.table, e.oldValues); err != nil {
		return "", err
	}

	query := "DELETE FROM " + QuotedTableNameFromString(schemaName, tableName) +
		" WHERE " + buildStringMapForWhere(e.table.Columns, e.oldValues)

	return query, nil
}

func (e *BinlogDeleteEvent) PaginationKey() (uint64, error) {
	return paginationKeyFromEventData(e.table, e.oldValues)
}

func NewBinlogDMLEvents(table *TableSchema, ev *replication.BinlogEvent, pos, resumablePos mysql.Position, query []byte) ([]DMLEvent, error) {
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
			if col.IsUnsigned && col.Type != schema.TYPE_MEDIUM_INT {
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
			} else if col.IsUnsigned && col.Type == schema.TYPE_MEDIUM_INT {
				if row[i] != nil {
					val := row[i].(int32)
					row[i] = NewUint24(val).Uint32()
				}
			}
		}
	}

	timestamp := time.Unix(int64(ev.Header.Timestamp), 0)
	eventBase := NewDMLEventBase(table, pos, resumablePos, query, timestamp)
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return NewBinlogInsertEvents(eventBase, rowsEvent)
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return NewBinlogDeleteEvents(eventBase, rowsEvent)
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return NewBinlogUpdateEvents(eventBase, rowsEvent)
	default:
		return nil, fmt.Errorf("unrecognized rows event: %s", ev.Header.EventType.String())
	}
}

func quotedColumnNames(table *TableSchema) []string {
	cols := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		cols[i] = QuoteField(column.Name)
	}

	return cols
}

func verifyValuesHasTheSameLengthAsColumns(table *TableSchema, values ...RowData) error {
	for _, v := range values {
		if len(table.Columns) != len(v) {
			return fmt.Errorf(
				"table %s.%s has %d columns but event has %d columns instead",
				table.Schema,
				table.Name,
				len(table.Columns),
				len(v),
			)
		}
	}
	return nil
}

func buildStringListForValues(columns []schema.TableColumn, values []interface{}) string {
	var buffer []byte

	for i, value := range values {
		if i > 0 {
			buffer = append(buffer, ',')
		}

		buffer = appendEscapedValue(buffer, value, columns[i])
	}

	return string(buffer)
}

func buildStringMapForWhere(columns []schema.TableColumn, values []interface{}) string {
	var buffer []byte

	for i, value := range values {
		if i > 0 {
			buffer = append(buffer, " AND "...)
		}

		buffer = append(buffer, QuoteField(columns[i].Name)...)

		if isNilValue(value) {
			// "WHERE value = NULL" will never match rows.
			buffer = append(buffer, " IS NULL"...)
		} else {
			buffer = append(buffer, '=')
			buffer = appendEscapedValue(buffer, value, columns[i])
		}
	}

	return string(buffer)
}

func buildStringMapForSet(columns []schema.TableColumn, values []interface{}) string {
	var buffer []byte

	for i, value := range values {
		if i > 0 {
			buffer = append(buffer, ',')
		}

		buffer = append(buffer, QuoteField(columns[i].Name)...)
		buffer = append(buffer, '=')
		buffer = appendEscapedValue(buffer, value, columns[i])
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

func appendEscapedValue(buffer []byte, value interface{}, column schema.TableColumn) []byte {
	if isNilValue(value) {
		return append(buffer, "NULL"...)
	}

	if uintv, ok := Uint64Value(value); ok {
		return strconv.AppendUint(buffer, uintv, 10)
	}

	if intv, ok := Int64Value(value); ok {
		return strconv.AppendInt(buffer, intv, 10)
	}

	switch v := value.(type) {
	case string:
		return appendEscapedString(buffer, v)
	case []byte:
		return appendEscapedBuffer(buffer, v, column.Type == schema.TYPE_JSON)
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
	case decimal.Decimal:
		return appendEscapedString(buffer, v.String())
	default:
		panic(fmt.Sprintf("unsupported type %t", value))
	}
}

func Uint64Value(value interface{}) (uint64, bool) {
	switch v := value.(type) {
	case uint64:
		return v, true
	case uint32:
		return uint64(v), true
	case uint16:
		return uint64(v), true
	case uint8:
		return uint64(v), true
	case uint:
		return uint64(v), true
	}
	return 0, false
}

func Int64Value(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int16:
		return int64(v), true
	case int8:
		return int64(v), true
	case int:
		return int64(v), true
	}
	return 0, false
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

func appendEscapedBuffer(buffer, value []byte, isJSON bool) []byte {
	if isJSON {
		// See https://bugs.mysql.com/bug.php?id=98496
		if len(value) == 0 {
			value = []byte("null")
		}

		buffer = append(buffer, "CAST("...)
	} else {
		buffer = append(buffer, "_binary"...)
	}

	buffer = append(buffer, '\'')

	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '\'' {
			buffer = append(buffer, '\'', '\'')
		} else {
			buffer = append(buffer, c)
		}
	}

	buffer = append(buffer, '\'')

	if isJSON {
		buffer = append(buffer, " AS JSON)"...)
	}

	return buffer
}

func paginationKeyFromEventData(table *TableSchema, rowData RowData) (uint64, error) {
	if err := verifyValuesHasTheSameLengthAsColumns(table, rowData); err != nil {
		return 0, err
	}

	return rowData.GetUint64(table.GetPaginationKeyIndex())
}
