package ghostferry

import (
	"context"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SqlDB opens lazily (no dial), so tests get real handles without a server.
func runtimeTestConfig(host string) *DatabaseConfig {
	return &DatabaseConfig{Host: host, Port: 3306, User: "root", Net: "tcp"}
}

// isClosed: a cancelled context makes PingContext fail fast without dialing.
func isClosed(t *testing.T, db *sql.DB) bool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := db.PingContext(ctx)
	require.Error(t, err)
	return err != context.Canceled
}

func TestSourceRuntimeAccessors(t *testing.T) {
	db := &sql.DB{Marginalia: "initial"}
	cfg := runtimeTestConfig("initial-host")

	rt := NewSourceRuntime(db, cfg)
	assert.Same(t, db, rt.DB())
	assert.Same(t, cfg, rt.Config())
}

func TestSourceRuntimeReplaceInstallsNewSourceAndRetiresOld(t *testing.T) {
	oldDB := &sql.DB{Marginalia: "old"}
	oldCfg := runtimeTestConfig("old-host")
	rt := NewSourceRuntime(oldDB, oldCfg)

	newCfg := runtimeTestConfig("new-host")
	newDB, err := rt.Replace(newCfg, nil)
	require.NoError(t, err)
	require.NotNil(t, newDB)

	assert.Same(t, newDB, rt.DB())
	assert.Same(t, newCfg, rt.Config())
	assert.NotSame(t, oldDB, rt.DB())

	assert.Contains(t, rt.retired, oldDB)
}

func TestSourceRuntimeReplaceRetainsAllPriorHandles(t *testing.T) {
	rt := NewSourceRuntime(&sql.DB{Marginalia: "gen0"}, runtimeTestConfig("h0"))

	_, err := rt.Replace(runtimeTestConfig("h1"), nil)
	require.NoError(t, err)
	_, err = rt.Replace(runtimeTestConfig("h2"), nil)
	require.NoError(t, err)

	assert.Len(t, rt.retired, 2)
}

func TestSourceRuntimeCloseRetiredClosesOnlyRetiredHandles(t *testing.T) {
	initial, err := runtimeTestConfig("gen0").SqlDB(nil)
	require.NoError(t, err)
	rt := NewSourceRuntime(initial, runtimeTestConfig("gen0"))

	replaced, err := rt.Replace(runtimeTestConfig("gen1"), nil)
	require.NoError(t, err)
	current, err := rt.Replace(runtimeTestConfig("gen2"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, current.Close()) })

	assert.False(t, isClosed(t, initial))
	assert.False(t, isClosed(t, replaced))

	rt.CloseRetired()
	assert.True(t, isClosed(t, initial), "initial handle must be closed")
	assert.True(t, isClosed(t, replaced), "intermediate handle must be closed")
	assert.False(t, isClosed(t, current), "current handle must stay open")
	assert.Same(t, current, rt.DB())

	rt.CloseRetired()
	assert.False(t, isClosed(t, current))
}

func TestSourceRuntimeNilInitial(t *testing.T) {
	rt := NewSourceRuntime(nil, nil)
	assert.Nil(t, rt.DB())
	assert.Nil(t, rt.Config())

	_, err := rt.Replace(runtimeTestConfig("h1"), nil)
	require.NoError(t, err)
	assert.Empty(t, rt.retired)
}
