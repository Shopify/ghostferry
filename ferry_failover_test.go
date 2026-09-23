package ghostferry

import (
	"errors"
	"testing"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The callback must never run when resolution or normalization already failed.
func unusedStreamVerifier(t *testing.T) func(*sql.DB, *DatabaseConfig) (mysql.GTIDSet, error) {
	return func(*sql.DB, *DatabaseConfig) (mysql.GTIDSet, error) {
		t.Error("stream verification must not run for an unresolved candidate")
		return nil, errors.New("unexpected stream verification")
	}
}

func newReconnectorFerry(host string) *Ferry {
	return &Ferry{
		Config:        &Config{Source: runtimeTestConfig(host)},
		logger:        LogWithField("tag", "test"),
		sourceRuntime: NewSourceRuntime(&sql.DB{Marginalia: host}, runtimeTestConfig(host)),
	}
}

func TestNewSourceReconnectorNilWhenDisabled(t *testing.T) {
	f := &Ferry{Config: &Config{}}
	assert.Nil(t, f.newSourceReconnector())

	f.Config.MasterFailoverRecovery = &MasterFailoverRecoveryConfig{}
	assert.Nil(t, f.newSourceReconnector(), "nil resolver disables reconnector")
}

func TestReconnectPropagatesResolverError(t *testing.T) {
	wantErr := errors.New("no master found")
	f := newReconnectorFerry("old")
	r := &ferrySourceReconnector{
		ferry:    f,
		resolver: MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) { return nil, wantErr }),
	}

	_, config, err := r.Reconnect(runtimeTestConfig("old"), nil, unusedStreamVerifier(t))
	assert.ErrorIs(t, err, wantErr)
	assert.Nil(t, config, "a failed resolution has no attempted candidate to report")
}

func TestReconnectRejectsNilCandidate(t *testing.T) {
	f := newReconnectorFerry("old")
	r := &ferrySourceReconnector{
		ferry:    f,
		resolver: MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) { return nil, nil }),
	}

	_, config, err := r.Reconnect(runtimeTestConfig("old"), nil, unusedStreamVerifier(t))
	assert.ErrorIs(t, err, errNilCandidate)
	assert.Nil(t, config)

	// The runtime must not have been swapped when the candidate is nil.
	assert.Equal(t, "old", f.sourceRuntime.Config().Host)
}

func TestReconnectRequiresStreamVerification(t *testing.T) {
	resolved := false
	f := newReconnectorFerry("old")
	r := &ferrySourceReconnector{
		ferry: f,
		resolver: MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) {
			resolved = true
			return runtimeTestConfig("new"), nil
		}),
	}

	_, _, err := r.Reconnect(runtimeTestConfig("old"), nil, nil)
	assert.ErrorIs(t, err, errNilStreamVerifier)
	assert.False(t, resolved, "a caller that cannot validate a stream must not even resolve")
	assert.Equal(t, "old", f.sourceRuntime.Config().Host)
}

func TestFailoverCandidateConfig(t *testing.T) {
	parseDSN := func(t *testing.T, cfg *DatabaseConfig) *gomysql.Config {
		t.Helper()
		mysqlCfg, err := cfg.MySQLConfig()
		require.NoError(t, err)
		parsed, err := gomysql.ParseDSN(mysqlCfg.FormatDSN())
		require.NoError(t, err)
		return parsed
	}

	t.Run("bare resolver candidate gains the mandatory session config", func(t *testing.T) {
		// A resolver typically knows only where the promoted writer lives.
		candidate := &DatabaseConfig{Host: "promoted", Port: 3307, User: "root", Pass: "hunter2"}

		normalized, err := normalizeFailoverCandidate(candidate)
		require.NoError(t, err)

		parsed := parseDSN(t, normalized)
		assert.Equal(t, "tcp", parsed.Net)
		assert.Equal(t, "promoted:3307", parsed.Addr)
		assert.Equal(t, "root", parsed.User)
		assert.Equal(t, "hunter2", parsed.Passwd)
		assert.Equal(t, "'+00:00'", parsed.Params["time_zone"])
		assert.Equal(t, "'STRICT_ALL_TABLES,NO_BACKSLASH_ESCAPES'", parsed.Params["sql_mode"])
		assert.Equal(t, DefaultMarginalia, normalized.Marginalia)

		// The resolver keeps ownership of what it returned.
		assert.Nil(t, candidate.Params, "normalization must not write into the resolver's config")
		assert.Equal(t, "", candidate.Net)
		assert.Equal(t, "", candidate.Marginalia)
	})

	t.Run("preserves configured connection settings", func(t *testing.T) {
		candidate := &DatabaseConfig{
			Host:         "promoted",
			Port:         3307,
			User:         "root",
			Net:          "tcp",
			Collation:    "utf8mb4_general_ci",
			Params:       map[string]string{"charset": "utf8mb4"},
			TLS:          &TLSConfig{CertPath: "/tmp/ca.pem", ServerName: "promoted"},
			ReadTimeout:  7,
			WriteTimeout: 11,
			Marginalia:   "application:custom",
		}
		params := candidate.Params

		normalized, err := normalizeFailoverCandidate(candidate)
		require.NoError(t, err)

		assert.Equal(t, "utf8mb4_general_ci", normalized.Collation)
		assert.Equal(t, "utf8mb4", normalized.Params["charset"])
		assert.Equal(t, uint64(7), normalized.ReadTimeout)
		assert.Equal(t, uint64(11), normalized.WriteTimeout)
		assert.Equal(t, "application:custom", normalized.Marginalia)
		require.NotNil(t, normalized.TLS)
		assert.NotSame(t, candidate.TLS, normalized.TLS, "TLS config must be copied, not shared")
		assert.Equal(t, "/tmp/ca.pem", normalized.TLS.CertPath)
		assert.Equal(t, "promoted", normalized.TLS.ServerName)

		// The resolver's map is neither replaced nor written to.
		assert.Len(t, params, 1)
		assert.Equal(t, map[string]string{"charset": "utf8mb4"}, candidate.Params)
	})

	t.Run("rejects conflicting session params without mutating the input", func(t *testing.T) {
		candidate := &DatabaseConfig{
			Host:   "promoted",
			Port:   3307,
			User:   "root",
			Params: map[string]string{"time_zone": "'+05:00'"},
		}

		_, err := normalizeFailoverCandidate(candidate)
		require.Error(t, err)
		assert.Equal(t, map[string]string{"time_zone": "'+05:00'"}, candidate.Params)
		assert.Equal(t, "", candidate.Net)
	})

	t.Run("rejects an unusable candidate", func(t *testing.T) {
		_, err := normalizeFailoverCandidate(nil)
		assert.ErrorIs(t, err, errNilCandidate)

		_, err = normalizeFailoverCandidate(&DatabaseConfig{Port: 3306, User: "root"})
		assert.Error(t, err, "a candidate without a host is not usable")

		_, err = normalizeFailoverCandidate(&DatabaseConfig{Host: "promoted", User: "root"})
		assert.Error(t, err, "a TCP candidate needs a port")
	})

	t.Run("is idempotent", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			input    *DatabaseConfig
			wantAddr string
			wantNet  string
			wantPort uint16
		}{
			{"tcp", &DatabaseConfig{Host: "promoted", Port: 3307, User: "root"}, "promoted:3307", "tcp", 3307},
			{"unix", &DatabaseConfig{Host: "/tmp/mysql.sock", Port: 1234, Net: "unix", User: "root"}, "/tmp/mysql.sock", "unix", 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				first, err := normalizeFailoverCandidate(tc.input)
				require.NoError(t, err)

				// A resolver may hand back a config it already validated.
				second, err := normalizeFailoverCandidate(first)
				require.NoError(t, err)

				for _, cfg := range []*DatabaseConfig{first, second} {
					assert.Equal(t, tc.wantPort, cfg.Port)
					assert.Equal(t, tc.wantNet, cfg.Net)
					parsed := parseDSN(t, cfg)
					assert.Equal(t, tc.wantAddr, parsed.Addr)
					assert.Equal(t, tc.wantNet, parsed.Net)
					assert.Equal(t, "'+00:00'", parsed.Params["time_zone"])
				}
			})
		}
	})
}

// The target streamer shares the Ferry config but must never reach the
// source resolver.
func TestSourceFailoverWiring(t *testing.T) {
	sentinel := errors.New("no promoted master available")
	lost := errors.New("connection lost")

	resolverCalls := 0
	f := &Ferry{
		Config: &Config{
			Source:               runtimeTestConfig("source"),
			Target:               runtimeTestConfig("target"),
			BinlogCoordinateMode: BinlogCoordinateGTID,
			MyServerId:           4242,
			MasterFailoverRecovery: &MasterFailoverRecoveryConfig{
				MaxAttempts: 2,
				RetryWait:   time.Millisecond,
				Resolver: MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) {
					resolverCalls++
					return nil, sentinel
				}),
			},
		},
		logger: LogWithField("tag", "test"),
	}
	f.OverallState.Store(StateStarting)
	f.SourceDB = &sql.DB{Marginalia: "source"}
	f.TargetDB = &sql.DB{Marginalia: "target"}
	f.sourceRuntime = NewSourceRuntime(f.SourceDB, f.Config.Source)

	runUntilDisconnect := func(s *BinlogStreamer) *recordingGTIDErrorHandler {
		handler := &recordingGTIDErrorHandler{}
		s.ErrorHandler = handler
		stream := replication.NewBinlogStreamer()
		s.binlogStreamer = stream
		s.binlogSyncer = &failoverTestSyncer{start: func(mysql.GTIDSet) (*replication.BinlogStreamer, error) {
			return stream, nil
		}}
		s.seedGTIDSets(mustParseGTID(t, gtidSetLower))
		require.True(t, stream.AddErrorToStreamer(lost))
		s.Run()
		return handler
	}

	sourceHandler := runUntilDisconnect(f.NewSourceBinlogStreamer())
	require.Error(t, sourceHandler.err)
	assert.ErrorIs(t, sourceHandler.err, sentinel, "the source stream must recover through the configured resolver")
	assert.Equal(t, 2, resolverCalls, "recovery must respect the configured attempt budget")

	targetStreamer, err := f.NewTargetBinlogStreamer()
	require.NoError(t, err)
	targetHandler := runUntilDisconnect(targetStreamer)
	require.Error(t, targetHandler.err)
	assert.ErrorIs(t, targetHandler.err, lost, "a lost target stream is fatal, not a failover")
	assert.Equal(t, 2, resolverCalls, "the target stream must never consult the source resolver")
}

// The resolver discovers a writer, not a replica plus its upstream master.
func TestMasterFailoverRecoveryRejectsReplicaSource(t *testing.T) {
	f := &Ferry{
		Config: &Config{
			Source:               runtimeTestConfig("source"),
			Target:               runtimeTestConfig("target"),
			BinlogCoordinateMode: BinlogCoordinateGTID,
			MasterFailoverRecovery: &MasterFailoverRecoveryConfig{
				Resolver: MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) {
					return nil, nil
				}),
			},
		},
		ErrorHandler:                       &recordingGTIDErrorHandler{},
		WaitUntilReplicaIsCaughtUpToMaster: &WaitUntilReplicaIsCaughtUpToMaster{},
	}

	err := f.Initialize()
	require.Error(t, err)
	assert.Nil(t, f.SourceDB, "the combination must be rejected before any connection is opened")
	assert.Nil(t, f.SourceRuntime())
}
