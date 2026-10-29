package ghostferry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"

	sqlwrapper "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

// These fixtures exercise the SQL scan path without requiring a MySQL server.
// MySQL exposes fingerprint-query keys as decimal bytes, including unsigned
// values above MaxInt64, so the fixture deliberately returns []byte values.
type paginationVerifierFixture struct {
	columns      []string
	values       [][]driver.Value
	expectedArgs []driver.Value
}

func (f *paginationVerifierFixture) Connect(context.Context) (driver.Conn, error) {
	return &paginationVerifierConn{fixture: f}, nil
}
func (f *paginationVerifierFixture) Driver() driver.Driver { return paginationVerifierDriver{} }

type paginationVerifierDriver struct{}

func (paginationVerifierDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use the fixture connector")
}

type paginationVerifierConn struct{ fixture *paginationVerifierFixture }

func (c *paginationVerifierConn) Prepare(string) (driver.Stmt, error) {
	return &paginationVerifierStmt{fixture: c.fixture}, nil
}
func (*paginationVerifierConn) Close() error { return nil }
func (*paginationVerifierConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions not supported")
}

type paginationVerifierStmt struct{ fixture *paginationVerifierFixture }

func (*paginationVerifierStmt) Close() error  { return nil }
func (*paginationVerifierStmt) NumInput() int { return -1 }
func (*paginationVerifierStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("writes not supported")
}
func (s *paginationVerifierStmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.fixture.expectedArgs != nil && !reflect.DeepEqual(s.fixture.expectedArgs, args) {
		return nil, errors.New("pagination arguments changed during reverification")
	}
	return &paginationVerifierRows{fixture: s.fixture}, nil
}

type paginationVerifierRows struct {
	fixture *paginationVerifierFixture
	index   int
}

func (r *paginationVerifierRows) Columns() []string { return r.fixture.columns }
func (*paginationVerifierRows) Close() error        { return nil }
func (r *paginationVerifierRows) Next(dest []driver.Value) error {
	if r.index == len(r.fixture.values) {
		return io.EOF
	}
	copy(dest, r.fixture.values[r.index])
	r.index++
	return nil
}

func paginationVerifierDB(t *testing.T, fixture *paginationVerifierFixture) *sqlwrapper.DB {
	db := sql.OpenDB(fixture)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return &sqlwrapper.DB{DB: db}
}

func paginationVerifierTable(column schema.TableColumn) *TableSchema {
	column.Name = "id"
	return &TableSchema{
		Table: &schema.Table{
			Schema: "gftest", Name: "signed_keys",
			Columns:   []schema.TableColumn{column, {Name: "payload", Type: schema.TYPE_STRING}},
			PKColumns: []int{0},
		},
		PaginationKeyColumn: &column,
	}
}

func paginationVerifierKeyString(column schema.TableColumn, raw string) string {
	if column.Type == schema.TYPE_BINARY || column.Type == schema.TYPE_STRING {
		return hex.EncodeToString([]byte(raw))
	}
	return raw
}

func TestPaginationVerifierFingerprintKeys(t *testing.T) {
	for _, tc := range []struct {
		name   string
		column schema.TableColumn
		keys   []string
	}{
		{"signed", schema.TableColumn{Type: schema.TYPE_NUMBER}, []string{"-9223372036854775808", "-7", "0", "4", "9223372036854775807"}},
		{"mediumint", schema.TableColumn{Type: schema.TYPE_MEDIUM_INT}, []string{"-8388608", "-1", "0", "8388607"}},
		{"unsigned", schema.TableColumn{Type: schema.TYPE_NUMBER, IsUnsigned: true}, []string{"0", "9223372036854775808", "18446744073709551615"}},
		{"binary", schema.TableColumn{Type: schema.TYPE_BINARY}, []string{"-7", "\x00\xff\x80"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := paginationVerifierTable(tc.column)
			table.CompressedColumnsForVerification = map[string]string{"payload": CompressionSnappy}
			fixture := &paginationVerifierFixture{columns: []string{"id", "fingerprint", "payload"}}
			keys := make([]interface{}, len(tc.keys))
			for i, key := range tc.keys {
				keys[i] = key
				fixture.values = append(fixture.values, []driver.Value{[]byte(key), []byte("hash:" + key), snappy.Encode(nil, []byte("data:"+key))})
			}
			v := &InlineVerifier{}
			hashes, decompressed, err := v.getFingerprintDataFromDb(paginationVerifierDB(t, fixture), NewStmtCache(), table.Schema, table.Name, nil, table, keys)
			require.NoError(t, err)
			require.Len(t, hashes, len(tc.keys))
			for _, key := range tc.keys {
				mapKey := paginationVerifierKeyString(tc.column, key)
				require.Equal(t, []byte("hash:"+key), hashes[mapKey])
				require.Equal(t, []byte("data:"+key), decompressed[mapKey]["payload"])
			}
		})
	}
}

func TestPaginationCompressionVerifierKeys(t *testing.T) {
	for _, tc := range []struct {
		name   string
		column schema.TableColumn
		key    string
	}{
		{"signed", schema.TableColumn{Type: schema.TYPE_NUMBER}, "-9223372036854775808"},
		{"unsigned", schema.TableColumn{Type: schema.TYPE_NUMBER, IsUnsigned: true}, "18446744073709551615"},
		{"binary", schema.TableColumn{Type: schema.TYPE_BINARY}, "\x00\xff-7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := paginationVerifierTable(tc.column)
			verifier, err := NewCompressionVerifier(TableColumnCompressionConfig{table.Name: {"payload": CompressionSnappy}}, TableSchemaCache{table.String(): table})
			require.NoError(t, err)
			fixture := &paginationVerifierFixture{
				columns: []string{"id", "payload"},
				values:  [][]driver.Value{{[]byte(tc.key), snappy.Encode(nil, []byte("payload"))}},
			}
			hashes, err := verifier.GetCompressedHashes(paginationVerifierDB(t, fixture), table.Schema, table.Name, "id", table.Columns[1:], []interface{}{tc.key})
			require.NoError(t, err)
			want, err := verifier.HashRow([][]byte{[]byte("payload")})
			require.NoError(t, err)
			require.Equal(t, map[string][]byte{paginationVerifierKeyString(tc.column, tc.key): want}, hashes)
		})
	}
}

func TestPaginationBinlogKeysAndReverification(t *testing.T) {
	for _, tc := range []struct {
		name   string
		column schema.TableColumn
		value  interface{}
		key    string
	}{
		{"negative", schema.TableColumn{Type: schema.TYPE_NUMBER}, int64(-1), "-1"},
		{"minimum", schema.TableColumn{Type: schema.TYPE_NUMBER}, int64(math.MinInt64), "-9223372036854775808"},
		{"zero", schema.TableColumn{Type: schema.TYPE_NUMBER}, int64(0), "0"},
		{"unsigned", schema.TableColumn{Type: schema.TYPE_NUMBER, IsUnsigned: true}, uint64(math.MaxUint64), "18446744073709551615"},
		{"binary", schema.TableColumn{Type: schema.TYPE_BINARY}, []byte("\x00\xff-7"), "\x00\xff-7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := paginationVerifierTable(tc.column)
			mapKey := paginationVerifierKeyString(tc.column, tc.key)
			// Row copying normally initializes this cache before reverification.
			// Initialize it before the source and target queries run concurrently.
			table.RowMd5Query()
			base := &DMLEventBase{table: table}
			events := []DMLEvent{
				&BinlogInsertEvent{DMLEventBase: base, newValues: RowData{tc.value, "inserted"}},
				&BinlogUpdateEvent{DMLEventBase: base, oldValues: RowData{tc.value, "inserted"}, newValues: RowData{tc.value, "updated"}},
				&BinlogDeleteEvent{DMLEventBase: base, oldValues: RowData{tc.value, "updated"}},
			}
			for _, event := range events {
				key, err := event.PaginationKey()
				require.NoError(t, err)
				require.Equal(t, mapKey, key)
			}
			source := &paginationVerifierFixture{
				columns: []string{"id", "fingerprint"}, values: [][]driver.Value{{[]byte(tc.key), []byte("correct")}},
				expectedArgs: []driver.Value{mapKey},
			}
			target := &paginationVerifierFixture{
				columns: source.columns, values: [][]driver.Value{{[]byte(tc.key), []byte("incorrect")}},
				expectedArgs: source.expectedArgs,
			}
			v := &InlineVerifier{
				SourceDB: paginationVerifierDB(t, source), TargetDB: paginationVerifierDB(t, target),
				TableSchemaCache: TableSchemaCache{table.String(): table}, BatchSize: 2,
				reverifyStore: NewBinlogVerifyStore(), sourceStmtCache: NewStmtCache(), targetStmtCache: NewStmtCache(),
				logger: LogWithField("test", t.Name()),
			}
			require.NoError(t, v.binlogEventListener(events))
			require.Equal(t, 3, v.reverifyStore.Serialize()[table.Schema][table.Name][mapKey])
			found, mismatches, err := v.verifyAllEventsInStore()
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, mapKey, mismatches[table.Schema][table.Name][0].Pk)
			v.readdMismatchedPaginationKeysToBeVerifiedAgain(mismatches)
			require.Equal(t, 3, v.reverifyStore.Serialize()[table.Schema][table.Name][mapKey])
			target.values = source.values
			for v.reverifyStore.CurrentRowCount() > 0 {
				found, _, err = v.verifyAllEventsInStore()
				require.NoError(t, err)
				require.False(t, found)
			}
			require.Zero(t, v.reverifyStore.CurrentEntriesCount())
		})
	}
}
