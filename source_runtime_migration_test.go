package ghostferry

import (
	"context"
	sqlorig "database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests verify what source consumers observe when a SourceRuntime is
// configured: metadata reads and verification use the current runtime handle,
// new cursors bind to it at creation, and existing cursors / in-flight inline
// verification keep the handle they started with. No MySQL server is needed:
// fixture DBs answer every query with canned rows, and SourceRuntime.Replace
// opens a lazy (undialed) handle which tests close before querying so that a
// consumer routed to the replacement fails deterministically.

var errUnsupportedSourceRuntimeTestOp = errors.New("unsupported source runtime test operation")

// sourceRuntimeQueryConnector backs a *sql.DB that returns the configured rows
// for every query, whether issued directly or through a prepared statement.
type sourceRuntimeQueryConnector struct {
	columns []string
	data    [][]driver.Value
}

func (c *sourceRuntimeQueryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &sourceRuntimeQueryConn{connector: c}, nil
}

func (c *sourceRuntimeQueryConnector) Driver() driver.Driver { return sourceRuntimeQueryDriver{} }

func (c *sourceRuntimeQueryConnector) rows() *sourceRuntimeQueryRows {
	return &sourceRuntimeQueryRows{columns: c.columns, data: c.data}
}

type sourceRuntimeQueryDriver struct{}

func (sourceRuntimeQueryDriver) Open(string) (driver.Conn, error) {
	return nil, errUnsupportedSourceRuntimeTestOp
}

type sourceRuntimeQueryConn struct {
	connector *sourceRuntimeQueryConnector
}

func (c *sourceRuntimeQueryConn) Prepare(string) (driver.Stmt, error) {
	return &sourceRuntimeQueryStmt{connector: c.connector}, nil
}

func (c *sourceRuntimeQueryConn) Close() error { return nil }

func (c *sourceRuntimeQueryConn) Begin() (driver.Tx, error) {
	return nil, errUnsupportedSourceRuntimeTestOp
}

func (c *sourceRuntimeQueryConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.connector.rows(), nil
}

type sourceRuntimeQueryStmt struct {
	connector *sourceRuntimeQueryConnector
}

func (s *sourceRuntimeQueryStmt) Close() error  { return nil }
func (s *sourceRuntimeQueryStmt) NumInput() int { return -1 }

func (s *sourceRuntimeQueryStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errUnsupportedSourceRuntimeTestOp
}

func (s *sourceRuntimeQueryStmt) Query([]driver.Value) (driver.Rows, error) {
	return s.connector.rows(), nil
}

type sourceRuntimeQueryRows struct {
	columns []string
	data    [][]driver.Value
	next    int
}

func (r *sourceRuntimeQueryRows) Columns() []string { return r.columns }
func (r *sourceRuntimeQueryRows) Close() error      { return nil }

func (r *sourceRuntimeQueryRows) Next(dest []driver.Value) error {
	if r.next >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.next])
	r.next++
	return nil
}

// newSourceRuntimeQueryDB returns an open *sql.DB whose every query yields the
// given columns and rows. It is closed when the test ends.
func newSourceRuntimeQueryDB(t *testing.T, columns []string, data [][]driver.Value) *sql.DB {
	t.Helper()
	db := &sql.DB{DB: sqlorig.OpenDB(&sourceRuntimeQueryConnector{columns: columns, data: data})}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// replaceWithClosedHandle swaps rt to a fresh lazily-opened handle and closes
// that handle immediately, so any consumer that resolves the current source
// after the swap fails deterministically without dialing. It requires the
// swap to have actually installed a new handle so a no-op Replace cannot pass.
func replaceWithClosedHandle(t *testing.T, rt *SourceRuntime, host string) *sql.DB {
	t.Helper()
	previous := rt.DB()
	replacement, err := rt.Replace(runtimeTestConfig(host), nil)
	require.NoError(t, err)
	require.NotNil(t, replacement)
	require.NotSame(t, previous, replacement, "Replace must install a new handle")
	require.Same(t, replacement, rt.DB())
	t.Cleanup(rt.CloseRetired)
	t.Cleanup(func() { _ = replacement.Close() })
	require.NoError(t, replacement.Close())
	return replacement
}

// numericReviewTable is a review.t table paginated on a numeric id column.
func numericReviewTable() *TableSchema {
	columns := []schema.TableColumn{{Name: "id", Type: schema.TYPE_NUMBER}}
	return &TableSchema{
		Table:               &schema.Table{Schema: "review", Name: "t", Columns: columns},
		PaginationKeyColumn: &columns[0],
		PaginationKeyIndex:  0,
	}
}

func newTestCursorConfig(db *sql.DB, rt *SourceRuntime) *CursorConfig {
	batchSize := uint64(1)
	return &CursorConfig{DB: db, SourceRuntime: rt, BatchSize: &batchSize, ReadRetries: 1}
}

func newSingleRowDB(t *testing.T) *sql.DB {
	t.Helper()
	return newSourceRuntimeQueryDB(t, []string{"id"}, [][]driver.Value{{int64(1)}})
}

func scanCursor(cursor *Cursor) ([]RowData, error) {
	var rows []RowData
	err := cursor.Each(func(batch *RowBatch) error {
		rows = append(rows, batch.Values()...)
		return nil
	})
	return rows, err
}

var singleRow = []RowData{{int64(1)}}

// ---------------------------------------------------------------------------
// MaxTableSizeSorter

var sorterColumns = []string{"TABLE_NAME", "TABLE_SCHEMA"}

func sorterInput() (map[*TableSchema]PaginationKey, *TableSchema, *TableSchema) {
	small := &TableSchema{Table: &schema.Table{Schema: "review", Name: "small"}}
	large := &TableSchema{Table: &schema.Table{Schema: "review", Name: "large"}}
	return map[*TableSchema]PaginationKey{
		small: NewUint64Key(10),
		large: NewUint64Key(20),
	}, small, large
}

func assertSortedTables(t *testing.T, sorted []TableMaxPaginationKey, expected ...*TableSchema) {
	t.Helper()
	require.Len(t, sorted, len(expected))
	for i, table := range expected {
		assert.Same(t, table, sorted[i].Table, "position %d", i)
	}
	for _, entry := range sorted {
		switch entry.Table.Name {
		case "small":
			assert.Equal(t, NewUint64Key(10), entry.MaxPaginationKey)
		case "large":
			assert.Equal(t, NewUint64Key(20), entry.MaxPaginationKey)
		default:
			t.Fatalf("unexpected table %s", entry.Table.Name)
		}
	}
}

func TestMaxTableSizeSorterUsesSourceRuntime(t *testing.T) {
	input, small, large := sorterInput()

	staticDB := newSourceRuntimeQueryDB(t, sorterColumns, [][]driver.Value{
		{"small", "review"},
		{"large", "review"},
	})
	runtimeDB := newSourceRuntimeQueryDB(t, sorterColumns, [][]driver.Value{
		{"large", "review"},
		{"small", "review"},
	})
	rt := NewSourceRuntime(runtimeDB, runtimeTestConfig("runtime"))

	iterator := &DataIterator{DB: staticDB, SourceRuntime: rt}
	sorter := MaxTableSizeSorter{DataIterator: iterator}

	// The runtime's ordering wins over the static DB's.
	sorted, err := sorter.Sort(input)
	require.NoError(t, err)
	assertSortedTables(t, sorted, large, small)

	// The sorter resolves the source on every call: once the runtime points at
	// an unusable handle, sorting fails rather than falling back to the static
	// DB, which is still open.
	replaceWithClosedHandle(t, rt, "replacement")
	_, err = sorter.Sort(input)
	require.Error(t, err)
}

func TestMaxTableSizeSorterStaticFallback(t *testing.T) {
	input, small, large := sorterInput()

	cases := map[string]*SourceRuntime{
		"no runtime":          nil,
		"runtime with nil DB": NewSourceRuntime(nil, nil),
	}
	for name, rt := range cases {
		t.Run(name, func(t *testing.T) {
			staticDB := newSourceRuntimeQueryDB(t, sorterColumns, [][]driver.Value{
				{"large", "review"},
				{"small", "review"},
			})
			sorter := MaxTableSizeSorter{DataIterator: &DataIterator{DB: staticDB, SourceRuntime: rt}}

			sorted, err := sorter.Sort(input)
			require.NoError(t, err)
			assertSortedTables(t, sorted, large, small)
		})
	}
}

// ---------------------------------------------------------------------------
// Cursor

func TestNewCursorBindsToResolvedDB(t *testing.T) {
	table := numericReviewTable()
	initialDB := newSingleRowDB(t)
	rt := NewSourceRuntime(initialDB, runtimeTestConfig("old"))
	cfg := newTestCursorConfig(initialDB, rt)

	// A cursor created before the swap keeps scanning the server it started on.
	before := cfg.NewCursorWithoutRowLock(table, NewUint64Key(0), NewUint64Key(1))

	replaceWithClosedHandle(t, rt, "new")

	rows, err := scanCursor(before)
	require.NoError(t, err)
	assert.Equal(t, singleRow, rows, "existing cursor must keep its handle")

	// A cursor created after the swap uses the current (closed) runtime handle
	// rather than the still-open static DB.
	after := cfg.NewCursorWithoutRowLock(table, NewUint64Key(0), NewUint64Key(1))
	rows, err = scanCursor(after)
	require.Error(t, err)
	assert.Empty(t, rows)
}

func TestCursorRuntimeFallback(t *testing.T) {
	table := numericReviewTable()

	cases := map[string]*SourceRuntime{
		"no runtime":          nil,
		"runtime with nil DB": NewSourceRuntime(nil, nil),
	}
	for name, rt := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := newTestCursorConfig(newSingleRowDB(t), rt)
			cursor := cfg.NewCursorWithoutRowLock(table, NewUint64Key(0), NewUint64Key(1))

			rows, err := scanCursor(cursor)
			require.NoError(t, err)
			assert.Equal(t, singleRow, rows)
		})
	}
}

// ---------------------------------------------------------------------------
// InlineVerifier

var (
	fingerprintColumns = []string{"id", "__ghostferry_row_md5"}
	oldFingerprint     = []byte("old-fingerprint")
)

func newFingerprintDB(t *testing.T) *sql.DB {
	t.Helper()
	return newSourceRuntimeQueryDB(t, fingerprintColumns, [][]driver.Value{{int64(1), oldFingerprint}})
}

func TestInlineVerifierSourceRuntimeCacheLifetime(t *testing.T) {
	table := numericReviewTable()
	key := NewUint64Key(1).String()
	paginationKeys := []interface{}{uint64(1)}

	oldDB := newFingerprintDB(t)
	rt := NewSourceRuntime(oldDB, runtimeTestConfig("old"))
	v := &InlineVerifier{SourceDB: oldDB, SourceRuntime: rt, sourceStmtCache: NewStmtCache()}

	// Prime the source statement cache against the old DB.
	fingerprints, _, err := v.getFingerprintDataFromSourceDb("review", "t", nil, table, paginationKeys)
	require.NoError(t, err)
	assert.Equal(t, oldFingerprint, fingerprints[key])

	// An in-flight worker holds the (db, cache) pair it resolved before the swap.
	inFlightDB, inFlightCache := v.currentSourceDB()

	replaceWithClosedHandle(t, rt, "new")

	// New work follows the runtime to the (closed) replacement. Reusing the
	// query-keyed cache primed against the old DB would incorrectly succeed.
	_, _, err = v.getFingerprintDataFromSourceDb("review", "t", nil, table, paginationKeys)
	require.Error(t, err)

	// The in-flight pair remains usable until retired handles are released.
	fingerprints, _, err = v.getFingerprintDataFromDb(inFlightDB, inFlightCache, "review", "t", nil, table, paginationKeys)
	require.NoError(t, err)
	assert.Equal(t, oldFingerprint, fingerprints[key])

	rt.CloseRetired()
	_, _, err = v.getFingerprintDataFromDb(inFlightDB, inFlightCache, "review", "t", nil, table, paginationKeys)
	require.Error(t, err)
}

func TestInlineVerifierSourceFallback(t *testing.T) {
	table := numericReviewTable()
	key := NewUint64Key(1).String()

	cases := map[string]*SourceRuntime{
		"no runtime":          nil,
		"runtime with nil DB": NewSourceRuntime(nil, nil),
	}
	for name, rt := range cases {
		t.Run(name, func(t *testing.T) {
			v := &InlineVerifier{SourceDB: newFingerprintDB(t), SourceRuntime: rt, sourceStmtCache: NewStmtCache()}

			fingerprints, _, err := v.getFingerprintDataFromSourceDb("review", "t", nil, table, []interface{}{uint64(1)})
			require.NoError(t, err)
			assert.Equal(t, oldFingerprint, fingerprints[key])
		})
	}
}

// ---------------------------------------------------------------------------
// Verifier validation

// sourceSelection enumerates the source configurations a verifier can be given.
type sourceSelection struct {
	name      string
	static    bool // provide a working static SourceDB
	runtime   bool // provide a SourceRuntime
	runtimeDB bool // ... whose handle is a working DB (false: nil handle)
	target    bool // provide a working TargetDB
	ok        bool
}

var sourceSelections = []sourceSelection{
	{name: "runtime only", runtime: true, runtimeDB: true, target: true, ok: true},
	{name: "static only", static: true, target: true, ok: true},
	{name: "nil-handle runtime with static fallback", static: true, runtime: true, target: true, ok: true},
	{name: "no source", target: true},
	{name: "nil-handle runtime without fallback", runtime: true, target: true},
	{name: "no target", static: true, runtime: true, runtimeDB: true},
}

func (s sourceSelection) build(t *testing.T, newDB func(*testing.T) *sql.DB) (static *sql.DB, rt *SourceRuntime, target *sql.DB) {
	t.Helper()
	if s.static {
		static = newDB(t)
	}
	if s.runtime {
		var db *sql.DB
		if s.runtimeDB {
			db = newDB(t)
		}
		rt = NewSourceRuntime(db, runtimeTestConfig("runtime"))
	}
	if s.target {
		target = newDB(t)
	}
	return
}

func TestIterativeVerifierSourceRuntimeValidation(t *testing.T) {
	for _, tc := range sourceSelections {
		t.Run(tc.name, func(t *testing.T) {
			static, rt, target := tc.build(t, newSingleRowDB)
			v := &IterativeVerifier{
				SourceDB:       static,
				SourceRuntime:  rt,
				TargetDB:       target,
				CursorConfig:   newTestCursorConfig(static, rt),
				BinlogStreamer: &BinlogStreamer{},
				Concurrency:    1,
			}

			err := v.Initialize()
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

var checksumColumns = []string{"Table", "Checksum"}

func newChecksumDB(checksum int64) func(*testing.T) *sql.DB {
	return func(t *testing.T) *sql.DB {
		t.Helper()
		return newSourceRuntimeQueryDB(t, checksumColumns, [][]driver.Value{{"review.t", checksum}})
	}
}

func runChecksumVerifier(t *testing.T, static *sql.DB, rt *SourceRuntime, target *sql.DB) error {
	t.Helper()
	v := &ChecksumTableVerifier{
		Tables:        []*TableSchema{numericReviewTable()},
		SourceDB:      static,
		SourceRuntime: rt,
		TargetDB:      target,
	}

	if err := v.StartInBackground(); err != nil {
		return err
	}
	v.Wait()

	result, err := v.Result()
	require.NoError(t, err)
	assert.True(t, result.DataCorrect)
	return nil
}

func TestChecksumVerifierSourceRuntimeValidation(t *testing.T) {
	for _, tc := range sourceSelections {
		t.Run(tc.name, func(t *testing.T) {
			static, rt, target := tc.build(t, newChecksumDB(11))
			err := runChecksumVerifier(t, static, rt, target)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}

	t.Run("runtime over static", func(t *testing.T) {
		static := newChecksumDB(22)(t)
		rt := NewSourceRuntime(newChecksumDB(11)(t), runtimeTestConfig("runtime"))
		target := newChecksumDB(11)(t)
		require.NoError(t, runChecksumVerifier(t, static, rt, target))
	})
}
