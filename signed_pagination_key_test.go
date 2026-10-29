package ghostferry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"testing"

	"github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ PaginationKey = Int64Key{}

func TestInt64Key(t *testing.T) {
	for _, value := range []int64{math.MinInt64, math.MinInt64 + 1, -1, 0, 1, 1<<53 + 1, math.MaxInt64 - 1, math.MaxInt64} {
		t.Run(strconv.FormatInt(value, 10), func(t *testing.T) {
			key := NewInt64KeyWithColumn("id", value)
			assert.Equal(t, value, key.SQLValue())
			assert.Equal(t, "id", key.ColumnName())
			assert.Equal(t, strconv.FormatInt(value, 10), key.String())
			assert.Equal(t, float64(value), key.NumericPosition())
			assert.Equal(t, value == math.MaxInt64, key.IsMax())

			for _, original := range []Int64Key{key, NewInt64Key(value)} {
				data, err := json.Marshal(original)
				require.NoError(t, err)
				assert.Contains(t, string(data), `"type":"int64"`)
				assert.Contains(t, string(data), `"value":`+strconv.FormatInt(value, 10))
				if original.Column == "" {
					assert.NotContains(t, string(data), `"column"`)
				}
				decoded, err := UnmarshalPaginationKey(data)
				require.NoError(t, err)
				assert.Equal(t, original, decoded)
			}
		})
	}
}

func TestIntegerPaginationKeyCompareExact(t *testing.T) {
	keys := []PaginationKey{
		NewInt64Key(math.MinInt64), NewInt64Key(math.MinInt64 + 1),
		NewInt64Key(-2), NewInt64Key(-1), NewInt64Key(0), NewUint64Key(0),
		NewInt64Key(1), NewUint64Key(1),
		NewInt64Key(1 << 53), NewInt64Key(1<<53 + 1), NewUint64Key(1<<53 + 1),
		NewInt64Key(math.MaxInt64 - 1), NewUint64Key(math.MaxInt64 - 1),
		NewInt64Key(math.MaxInt64), NewUint64Key(math.MaxInt64),
		NewUint64Key(uint64(math.MaxInt64) + 1), NewUint64Key(math.MaxUint64),
	}
	for _, left := range keys {
		for _, right := range keys {
			t.Run(fmt.Sprintf("%T_%s_vs_%T_%s", left, left, right, right), func(t *testing.T) {
				leftInt, ok := new(big.Int).SetString(left.String(), 10)
				require.True(t, ok)
				rightInt, ok := new(big.Int).SetString(right.String(), 10)
				require.True(t, ok)
				assert.Equal(t, leftInt.Cmp(rightInt), left.Compare(right))
			})
		}
	}
	assert.Panics(t, func() { NewInt64Key(1).Compare(NewBinaryKey([]byte{1})) })
	assert.Panics(t, func() { NewBinaryKey([]byte{1}).Compare(NewInt64Key(1)) })
}

func TestIntegerPaginationKeyLegacyCheckpoint(t *testing.T) {
	for _, value := range []uint64{0, 1, 1<<53 + 1, math.MaxInt64, math.MaxUint64} {
		key, err := UnmarshalPaginationKey([]byte(fmt.Sprintf(`{"type":"uint64","value":%d,"column":"id"}`, value)))
		require.NoError(t, err)
		assert.Equal(t, NewUint64KeyWithColumn("id", value), key)
		assert.Equal(t, -1, NewInt64Key(-1).Compare(key))
		if value <= math.MaxInt64 {
			assert.Zero(t, NewInt64Key(int64(value)).Compare(key))
		}
		data, err := json.Marshal(key)
		require.NoError(t, err)
		decoded, err := UnmarshalPaginationKey(data)
		require.NoError(t, err)
		assert.Equal(t, key, decoded)
	}
}

func TestInt64KeyInvalidJSON(t *testing.T) {
	for _, data := range []string{
		`{"type":"int64","value":9223372036854775808}`,
		`{"type":"int64","value":-9223372036854775809}`,
		`{"type":"int64","value":1.5}`,
		`{"type":"int64","value":1e3}`,
		`{"type":"int64","value":"1"}`,
		`{"type":"int64","value":null}`,
		`{"type":"int64"}`,
		`{"type":"int64","value":true}`,
		`{"type":"int64","value":[]}`,
		`{"type":"int64","value":{}}`,
		`{"type":"int64","value":`,
	} {
		t.Run(data, func(t *testing.T) {
			key, err := UnmarshalPaginationKey([]byte(data))
			assert.Error(t, err)
			assert.Nil(t, key)
		})
	}
}

func TestIntegerPaginationKeyBounds(t *testing.T) {
	for _, columnType := range []int{schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT} {
		for _, unsigned := range []bool{false, true} {
			column := &schema.TableColumn{Name: "id", Type: columnType, IsUnsigned: unsigned}
			min, max := MinPaginationKey(column), MaxPaginationKey(column)
			if unsigned {
				assert.Equal(t, NewUint64KeyWithColumn("id", 0), min)
				assert.Equal(t, NewUint64KeyWithColumn("id", math.MaxUint64), max)
			} else {
				assert.Equal(t, NewInt64KeyWithColumn("id", math.MinInt64), min)
				assert.Equal(t, NewInt64KeyWithColumn("id", math.MaxInt64), max)
			}
			assert.Equal(t, -1, min.Compare(max))
			assert.True(t, max.IsMax())
			assert.False(t, min.IsMax())
		}
	}
}

func TestIntegerPaginationKeyFromRow(t *testing.T) {
	// SQL drivers use decimal []byte/string and int64; binlog events use all
	// signed and unsigned integer widths. Both paths must honor the schema.
	for _, columnType := range []int{schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT} {
		for _, unsigned := range []bool{false, true} {
			column := &schema.TableColumn{Name: "id", Type: columnType, IsUnsigned: unsigned}
			for _, value := range []interface{}{
				int(42), int8(42), int16(42), int32(42), int64(42),
				uint(42), uint8(42), uint16(42), uint32(42), uint64(42),
				[]byte("42"), "42",
			} {
				t.Run(fmt.Sprintf("type%d_unsigned%t_%T", columnType, unsigned, value), func(t *testing.T) {
					key, err := NewPaginationKeyFromRow(RowData{"other", value}, 1, column)
					require.NoError(t, err)
					if unsigned {
						assert.Equal(t, NewUint64KeyWithColumn("id", 42), key)
					} else {
						assert.Equal(t, NewInt64KeyWithColumn("id", 42), key)
					}
				})
			}
		}
	}

	tests := []struct {
		value    interface{}
		unsigned bool
		want     PaginationKey
	}{
		{int(-1), false, NewInt64KeyWithColumn("id", -1)},
		{int8(-128), false, NewInt64KeyWithColumn("id", -128)},
		{int16(-32768), false, NewInt64KeyWithColumn("id", -32768)},
		{int32(math.MinInt32), false, NewInt64KeyWithColumn("id", math.MinInt32)},
		{int64(math.MinInt64), false, NewInt64KeyWithColumn("id", math.MinInt64)},
		{[]byte("-9223372036854775808"), false, NewInt64KeyWithColumn("id", math.MinInt64)},
		{"-9223372036854775808", false, NewInt64KeyWithColumn("id", math.MinInt64)},
		{uint64(math.MaxInt64), false, NewInt64KeyWithColumn("id", math.MaxInt64)},
		{"9223372036854775807", false, NewInt64KeyWithColumn("id", math.MaxInt64)},
		{int64(math.MaxInt64), true, NewUint64KeyWithColumn("id", math.MaxInt64)},
		{uint64(math.MaxUint64), true, NewUint64KeyWithColumn("id", math.MaxUint64)},
		{[]byte("18446744073709551615"), true, NewUint64KeyWithColumn("id", math.MaxUint64)},
		{"18446744073709551615", true, NewUint64KeyWithColumn("id", math.MaxUint64)},
		{0, false, NewInt64KeyWithColumn("id", 0)},
		{0, true, NewUint64KeyWithColumn("id", 0)},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%T_%v_unsigned%t", tt.value, tt.value, tt.unsigned), func(t *testing.T) {
			column := &schema.TableColumn{Name: "id", Type: schema.TYPE_NUMBER, IsUnsigned: tt.unsigned}
			key, err := NewPaginationKeyFromRow(RowData{tt.value}, 0, column)
			require.NoError(t, err)
			assert.Equal(t, tt.want, key)
		})
	}
}

func TestIntegerPaginationKeyFromRowRejectsInvalidValues(t *testing.T) {
	for _, unsigned := range []bool{false, true} {
		invalid := []interface{}{
			nil, float64(1), float32(1), true, []int{1},
			"", " ", " 1", "1 ", "1.0", "1e3", "0xff", "1_000", "abc", "--1", "++1",
			"-9223372036854775809", "18446744073709551616", []byte("bad"), []byte{},
		}
		if unsigned {
			invalid = append(invalid, int(-1), int8(-1), int16(-1), int32(-1), int64(-1), int64(math.MinInt64), "-1", []byte("-1"))
		} else {
			invalid = append(invalid, uint64(math.MaxInt64)+1, uint64(math.MaxUint64), "9223372036854775808", []byte("18446744073709551615"))
		}
		for _, value := range invalid {
			t.Run(fmt.Sprintf("unsigned%t_%T_%v", unsigned, value, value), func(t *testing.T) {
				column := &schema.TableColumn{Name: "id", Type: schema.TYPE_NUMBER, IsUnsigned: unsigned}
				key, err := NewPaginationKeyFromRow(RowData{value}, 0, column)
				assert.Error(t, err)
				assert.Nil(t, key)
			})
		}
	}
	for _, index := range []int{-1, 1} {
		_, err := NewPaginationKeyFromRow(RowData{1}, index, &schema.TableColumn{Type: schema.TYPE_NUMBER})
		assert.Error(t, err)
	}
}

func TestIntegerPaginationKeyMaxDatabaseScan(t *testing.T) {
	tests := []struct {
		name       string
		columnType int
		unsigned   bool
		value      driver.Value
		empty      bool
		want       PaginationKey
		wantError  bool
	}{
		{name: "all negative", value: int64(-1), want: NewInt64KeyWithColumn("id", -1)},
		{name: "minimum signed", value: int64(math.MinInt64), want: NewInt64KeyWithColumn("id", math.MinInt64)},
		{name: "maximum signed", value: []byte("9223372036854775807"), want: NewInt64KeyWithColumn("id", math.MaxInt64)},
		{name: "medium int negative", columnType: schema.TYPE_MEDIUM_INT, value: []byte("-8388608"), want: NewInt64KeyWithColumn("id", -8388608)},
		{name: "zero", value: int64(0), want: NewInt64KeyWithColumn("id", 0)},
		{name: "maximum unsigned", unsigned: true, value: []byte("18446744073709551615"), want: NewUint64KeyWithColumn("id", math.MaxUint64)},
		{name: "unsigned zero", unsigned: true, value: int64(0), want: NewUint64KeyWithColumn("id", 0)},
		{name: "binary", columnType: schema.TYPE_BINARY, value: []byte{0, 255}, want: NewBinaryKeyWithColumn("id", []byte{0, 255})},
		{name: "string", columnType: schema.TYPE_STRING, value: "abc", want: NewBinaryKeyWithColumn("id", []byte("abc"))},
		{name: "empty", empty: true},
		{name: "signed overflow", value: []byte("9223372036854775808"), wantError: true},
		{name: "unsigned overflow", unsigned: true, value: []byte("18446744073709551616"), wantError: true},
		{name: "unsigned negative", unsigned: true, value: int64(-1), wantError: true},
		{name: "null", value: nil, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columnType := tt.columnType
			if columnType == 0 {
				columnType = schema.TYPE_NUMBER
			}
			column := &schema.TableColumn{Name: "id", Type: columnType, IsUnsigned: tt.unsigned}
			table := &TableSchema{
				Table:               &schema.Table{Schema: "test", Name: "keys"},
				PaginationKeyColumn: column,
			}
			db := &sqlwrapper.DB{DB: sql.OpenDB(paginationKeyTestDriver{value: tt.value, empty: tt.empty})}
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			key, exists, err := maxPaginationKey(db, table)
			if tt.wantError {
				assert.Error(t, err)
				assert.False(t, exists)
				assert.Nil(t, key)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, !tt.empty, exists)
			assert.Equal(t, tt.want, key)
		})
	}
}

// Exercise database/sql scanning without requiring a running MySQL server.
type paginationKeyTestDriver struct {
	value driver.Value
	empty bool
}

func (d paginationKeyTestDriver) Open(string) (driver.Conn, error) {
	return paginationKeyTestConn{d}, nil
}

func (d paginationKeyTestDriver) Connect(context.Context) (driver.Conn, error) {
	return d.Open("")
}

func (d paginationKeyTestDriver) Driver() driver.Driver { return d }

type paginationKeyTestConn struct{ paginationKeyTestDriver }

func (c paginationKeyTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected Prepare")
}
func (c paginationKeyTestConn) Close() error { return nil }
func (c paginationKeyTestConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("unexpected Begin")
}
func (c paginationKeyTestConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != "SELECT `id` FROM `test`.`keys` ORDER BY `id` DESC LIMIT 1" || len(args) != 0 {
		return nil, fmt.Errorf("unexpected query: %s %v", query, args)
	}
	return &paginationKeyTestRows{value: c.value, done: c.empty}, nil
}

type paginationKeyTestRows struct {
	value driver.Value
	done  bool
}

func (r *paginationKeyTestRows) Columns() []string { return []string{"id"} }
func (r *paginationKeyTestRows) Close() error      { return nil }
func (r *paginationKeyTestRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	dest[0] = r.value
	r.done = true
	return nil
}
