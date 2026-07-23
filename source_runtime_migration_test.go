package ghostferry

import (
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests verify that source consumers resolve the current source
// connection from a SourceRuntime when one is configured, and fall back to the
// static DB otherwise. sql.DB values are sentinels; no query is executed.

func TestCursorConfigResolvedDB(t *testing.T) {
	staticDB := &sql.DB{Marginalia: "static"}

	// No runtime: uses static DB.
	c := &CursorConfig{DB: staticDB}
	assert.Same(t, staticDB, c.resolvedDB())

	// Runtime with a current DB: uses runtime DB.
	runtimeDB := &sql.DB{Marginalia: "runtime"}
	c.SourceRuntime = NewSourceRuntime(runtimeDB, runtimeTestConfig("h"))
	assert.Same(t, runtimeDB, c.resolvedDB())

	// Runtime present but nil DB: falls back to static DB.
	c.SourceRuntime = NewSourceRuntime(nil, nil)
	assert.Same(t, staticDB, c.resolvedDB())
}

func TestNewCursorBindsToResolvedDB(t *testing.T) {
	staticDB := &sql.DB{Marginalia: "static"}
	runtimeDB := &sql.DB{Marginalia: "runtime"}
	rt := NewSourceRuntime(staticDB, runtimeTestConfig("old"))

	cfg := &CursorConfig{DB: staticDB, SourceRuntime: rt}
	table := &TableSchema{}

	// Cursor created now binds to the current (static) handle.
	cursor := cfg.NewCursor(table, NewUint64Key(0), NewUint64Key(10))
	assert.Same(t, staticDB, cursor.DB)

	// After a runtime swap, a newly created cursor binds to the new handle,
	// while the already-created cursor keeps its handle (scan consistency).
	_, err := rt.Replace(runtimeTestConfig("new"), nil)
	require.NoError(t, err)
	newHandle := rt.DB()

	cursor2 := cfg.NewCursor(table, NewUint64Key(0), NewUint64Key(10))
	assert.Same(t, newHandle, cursor2.DB)
	assert.Same(t, staticDB, cursor.DB, "existing cursor must keep its handle")
	_ = runtimeDB
}

func TestInlineVerifierCurrentSourceDBResetsStmtCacheOnSwap(t *testing.T) {
	oldDB := &sql.DB{Marginalia: "old"}
	rt := NewSourceRuntime(oldDB, runtimeTestConfig("old"))
	v := &InlineVerifier{
		SourceDB:        oldDB,
		SourceRuntime:   rt,
		sourceStmtCache: NewStmtCache(),
	}

	origCache := v.sourceStmtCache
	db, cache := v.currentSourceDB()
	assert.Same(t, oldDB, db)
	assert.Same(t, origCache, cache, "no swap: cache unchanged")

	// Swap the runtime; the verifier must pick up the new DB and reset its
	// statement cache (statements were bound to the old DB).
	_, err := rt.Replace(runtimeTestConfig("new"), nil)
	require.NoError(t, err)
	newHandle := rt.DB()

	db, cache = v.currentSourceDB()
	assert.Same(t, newHandle, db)
	assert.NotSame(t, origCache, cache, "swap must reset the source statement cache")
}

func TestInlineVerifierCurrentSourceDBFallsBackWithoutRuntime(t *testing.T) {
	db := &sql.DB{Marginalia: "static"}
	v := &InlineVerifier{SourceDB: db, sourceStmtCache: NewStmtCache()}
	got, _ := v.currentSourceDB()
	assert.Same(t, db, got)
}

func TestIterativeVerifierCurrentSourceDB(t *testing.T) {
	staticDB := &sql.DB{Marginalia: "static"}
	v := &IterativeVerifier{SourceDB: staticDB}
	assert.Same(t, staticDB, v.currentSourceDB())

	runtimeDB := &sql.DB{Marginalia: "runtime"}
	v.SourceRuntime = NewSourceRuntime(runtimeDB, runtimeTestConfig("h"))
	assert.Same(t, runtimeDB, v.currentSourceDB())
}

func TestChecksumVerifierCurrentSourceDB(t *testing.T) {
	staticDB := &sql.DB{Marginalia: "static"}
	v := &ChecksumTableVerifier{SourceDB: staticDB}
	assert.Same(t, staticDB, v.currentSourceDB())

	runtimeDB := &sql.DB{Marginalia: "runtime"}
	v.SourceRuntime = NewSourceRuntime(runtimeDB, runtimeTestConfig("h"))
	assert.Same(t, runtimeDB, v.currentSourceDB())
}
