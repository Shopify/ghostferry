package ghostferry

import (
	"errors"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// swapTestConfig is a DatabaseConfig that SqlDB can open (lazily, without
// dialing). sql.Open does not connect, so swapSource can install a real
// Ferry-owned handle in unit tests against a bogus host.
func swapTestConfig(host string) *DatabaseConfig {
	return &DatabaseConfig{Host: host, Port: 3306, User: "root", Net: "tcp"}
}

// newFerryForSwapTest builds a minimal Ferry with the source-side consumers
// wired to a sentinel "old" DB handle so swapSource can be exercised without a
// live database.
func newFerryForSwapTest(oldDB *sql.DB) *Ferry {
	f := &Ferry{
		Config: &Config{
			Source: swapTestConfig("old-master"),
		},
		SourceDB: oldDB,
		logger:   LogWithField("tag", "test"),
		DataIterator: &DataIterator{
			DB:           oldDB,
			CursorConfig: &CursorConfig{DB: oldDB},
		},
		inlineVerifier: &InlineVerifier{SourceDB: oldDB, sourceStmtCache: NewStmtCache()},
	}
	return f
}

func TestSwapSourceRepointsAllConsumers(t *testing.T) {
	oldDB := &sql.DB{Marginalia: "old"}
	newConfig := swapTestConfig("new-master")
	oldConfig := swapTestConfig("old-master")

	f := newFerryForSwapTest(oldDB)
	oldStmtCache := f.inlineVerifier.sourceStmtCache
	iterativeVerifier := &IterativeVerifier{SourceDB: oldDB, CursorConfig: &CursorConfig{DB: oldDB}}
	f.Verifier = iterativeVerifier

	err := f.swapSource(MasterFailoverEvent{
		NewMasterConfig:      newConfig,
		NewMasterDB:          &sql.DB{Marginalia: "streamer-owned"},
		PreviousMasterConfig: oldConfig,
	})
	require.NoError(t, err)

	// Ferry opens its OWN handle (not the streamer's), so it must differ from
	// both the old handle and the streamer-owned one, and be consistently
	// installed everywhere.
	newDB := f.SourceDB
	require.NotNil(t, newDB)
	assert.NotSame(t, oldDB, newDB, "Ferry must install a fresh handle, not keep the old one")
	assert.Same(t, newConfig, f.Config.Source, "Config.Source must be the new master config")
	assert.Same(t, newDB, f.DataIterator.DB, "DataIterator.DB must be repointed")
	assert.Same(t, newDB, f.DataIterator.CursorConfig.DB, "CursorConfig.DB must be repointed")
	assert.Same(t, newDB, f.inlineVerifier.SourceDB, "inline verifier SourceDB must be repointed")
	assert.NotSame(t, oldStmtCache, f.inlineVerifier.sourceStmtCache, "inline verifier stmt cache must be reset")
	assert.Same(t, newDB, iterativeVerifier.SourceDB, "iterative verifier SourceDB must be repointed")
	assert.Same(t, newDB, iterativeVerifier.CursorConfig.DB, "iterative verifier CursorConfig.DB must be repointed")

	// The previous handle is retained (not closed) for later teardown.
	assert.Contains(t, f.previousSourceDBs, oldDB)
}

func TestSwapSourceChecksumVerifier(t *testing.T) {
	oldDB := &sql.DB{Marginalia: "old"}

	f := newFerryForSwapTest(oldDB)
	checksumVerifier := &ChecksumTableVerifier{SourceDB: oldDB}
	f.Verifier = checksumVerifier

	err := f.swapSource(MasterFailoverEvent{
		NewMasterConfig: swapTestConfig("new-master"),
	})
	require.NoError(t, err)

	assert.Same(t, f.SourceDB, checksumVerifier.SourceDB)
	assert.NotSame(t, oldDB, checksumVerifier.SourceDB)
}

func TestSwapSourceIsNilSafe(t *testing.T) {
	// A Ferry with no data iterator / verifiers must not panic.
	oldDB := &sql.DB{Marginalia: "old"}
	f := &Ferry{
		Config:   &Config{Source: swapTestConfig("old")},
		SourceDB: oldDB,
		logger:   LogWithField("tag", "test"),
	}

	assert.NotPanics(t, func() {
		err := f.swapSource(MasterFailoverEvent{NewMasterConfig: swapTestConfig("new")})
		require.NoError(t, err)
	})
	assert.NotSame(t, oldDB, f.SourceDB)
}

func TestClosePreviousSourceDBsDrains(t *testing.T) {
	// Use real (lazily-opened, undialed) handles so Close has an underlying
	// *sql.DB to act on.
	dbA, err := swapTestConfig("a").SqlDB(nil)
	require.NoError(t, err)
	dbB, err := swapTestConfig("b").SqlDB(nil)
	require.NoError(t, err)

	f := &Ferry{
		Config:            &Config{Source: swapTestConfig("old")},
		logger:            LogWithField("tag", "test"),
		previousSourceDBs: []*sql.DB{dbA, dbB},
	}
	assert.NotPanics(t, func() { f.closePreviousSourceDBs() })
	assert.Empty(t, f.previousSourceDBs, "retained handles must be drained")
}

func TestSwapSourceNilConfigIsNoop(t *testing.T) {
	oldDB := &sql.DB{Marginalia: "old"}
	f := newFerryForSwapTest(oldDB)
	err := f.swapSource(MasterFailoverEvent{NewMasterConfig: nil})
	require.NoError(t, err)
	assert.Same(t, oldDB, f.SourceDB, "a nil NewMasterConfig must leave the source untouched")
}

func TestFailoverRecoveryConfigForSourceComposesCallback(t *testing.T) {
	oldDB := &sql.DB{Marginalia: "old"}

	f := newFerryForSwapTest(oldDB)

	userCallbackCalled := false
	f.Config.MasterFailoverRecovery = &MasterFailoverRecoveryConfig{
		Resolver: MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) {
			return nil, nil
		}),
		OnFailover: func(ev MasterFailoverEvent) error {
			userCallbackCalled = true
			// By the time the user callback runs, the Ferry-wide swap must be done.
			assert.NotSame(t, oldDB, f.SourceDB, "Ferry swap must run before user callback")
			return nil
		},
	}

	cfg := f.failoverRecoveryConfigForSource()
	assert.NotNil(t, cfg.OnFailover)
	// The user config must not be mutated (its OnFailover is still the raw one).
	assert.NotNil(t, f.Config.MasterFailoverRecovery.OnFailover, "user config must not be mutated")

	err := cfg.OnFailover(MasterFailoverEvent{
		NewMasterConfig: swapTestConfig("new"),
	})
	assert.NoError(t, err)
	assert.True(t, userCallbackCalled, "user callback must be invoked after the swap")
	assert.NotSame(t, oldDB, f.SourceDB)
}

func TestFailoverRecoveryConfigForSourceNilWhenDisabled(t *testing.T) {
	f := &Ferry{Config: &Config{}}
	assert.Nil(t, f.failoverRecoveryConfigForSource())
}

func TestFailoverRecoveryConfigForSourcePropagatesUserError(t *testing.T) {
	oldDB := &sql.DB{Marginalia: "old"}
	f := newFerryForSwapTest(oldDB)
	wantErr := errors.New("user callback failed")
	f.Config.MasterFailoverRecovery = &MasterFailoverRecoveryConfig{
		Resolver: MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) {
			return nil, nil
		}),
		OnFailover: func(_ MasterFailoverEvent) error { return wantErr },
	}

	cfg := f.failoverRecoveryConfigForSource()
	err := cfg.OnFailover(MasterFailoverEvent{
		NewMasterConfig: swapTestConfig("new"),
	})
	assert.ErrorIs(t, err, wantErr)
}
