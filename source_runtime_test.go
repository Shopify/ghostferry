package ghostferry

import (
	"errors"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runtimeTestConfig is a DatabaseConfig SqlDB can open lazily (sql.Open does
// not dial), so Replace can install a real handle without a live server.
func runtimeTestConfig(host string) *DatabaseConfig {
	return &DatabaseConfig{Host: host, Port: 3306, User: "root", Net: "tcp"}
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

	// The runtime now serves the new handle/config.
	assert.Same(t, newDB, rt.DB())
	assert.Same(t, newCfg, rt.Config())
	assert.NotSame(t, oldDB, rt.DB())

	// The old handle is retired, not closed here.
	assert.Contains(t, rt.retired, oldDB)
}

func TestSourceRuntimeReplaceRetainsAllPriorHandles(t *testing.T) {
	rt := NewSourceRuntime(&sql.DB{Marginalia: "gen0"}, runtimeTestConfig("h0"))

	_, err := rt.Replace(runtimeTestConfig("h1"), nil)
	require.NoError(t, err)
	_, err = rt.Replace(runtimeTestConfig("h2"), nil)
	require.NoError(t, err)

	// Two swaps retire two handles (gen0 and the h1 handle).
	assert.Len(t, rt.retired, 2)
}

func TestSourceRuntimeCloseRetiredDrains(t *testing.T) {
	// Use real (lazily-opened, undialed) handles so Close has an underlying DB.
	dbA, err := runtimeTestConfig("a").SqlDB(nil)
	require.NoError(t, err)
	dbB, err := runtimeTestConfig("b").SqlDB(nil)
	require.NoError(t, err)

	rt := NewSourceRuntime(nil, nil)
	rt.retired = []*sql.DB{dbA, dbB}

	assert.NotPanics(t, func() { rt.CloseRetired() })
	assert.Empty(t, rt.retired, "retired handles must be drained")
}

func TestSourceRuntimeReplaceValidatedFailureDoesNotPublish(t *testing.T) {
	oldDB := &sql.DB{Marginalia: "old"}
	oldCfg := runtimeTestConfig("old-host")
	rt := NewSourceRuntime(oldDB, oldCfg)

	validateErr := errors.New("candidate rejected")
	_, err := rt.ReplaceValidated(runtimeTestConfig("bad-host"), nil, func(_ *sql.DB) error {
		return validateErr
	})
	assert.ErrorIs(t, err, validateErr)

	// The current source must be unchanged and nothing retired.
	assert.Same(t, oldDB, rt.DB())
	assert.Same(t, oldCfg, rt.Config())
	assert.Empty(t, rt.retired)
}

func TestSourceRuntimeReplaceValidatedSuccessPublishes(t *testing.T) {
	oldDB := &sql.DB{Marginalia: "old"}
	rt := NewSourceRuntime(oldDB, runtimeTestConfig("old-host"))

	validated := false
	newCfg := runtimeTestConfig("new-host")
	newDB, err := rt.ReplaceValidated(newCfg, nil, func(db *sql.DB) error {
		validated = true
		assert.NotNil(t, db)
		return nil
	})
	require.NoError(t, err)
	assert.True(t, validated)
	assert.Same(t, newDB, rt.DB())
	assert.Same(t, newCfg, rt.Config())
	assert.Contains(t, rt.retired, oldDB)
}

func TestSourceRuntimeNilInitial(t *testing.T) {
	rt := NewSourceRuntime(nil, nil)
	assert.Nil(t, rt.DB())
	assert.Nil(t, rt.Config())

	// Replacing from a nil initial does not retire anything.
	_, err := rt.Replace(runtimeTestConfig("h1"), nil)
	require.NoError(t, err)
	assert.Empty(t, rt.retired)
}
