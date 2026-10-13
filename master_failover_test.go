package ghostferry

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailoverRecoveryEnabled(t *testing.T) {
	reconnector := &fakeReconnector{}

	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	assert.False(t, s.failoverRecoveryEnabled())

	s.MasterFailoverRecovery = &MasterFailoverRecoveryConfig{}
	assert.False(t, s.failoverRecoveryEnabled())

	s.BinlogCoordinateMode = BinlogCoordinateFilePosition
	s.SourceReconnector = reconnector
	assert.False(t, s.failoverRecoveryEnabled())

	s.BinlogCoordinateMode = BinlogCoordinateGTID
	assert.True(t, s.failoverRecoveryEnabled())
}

func TestFailoverRetryWaitDefault(t *testing.T) {
	c := &MasterFailoverRecoveryConfig{}
	assert.Equal(t, DefaultFailoverRetryWait, c.retryWait())
	c.RetryWait = 2 * time.Second
	assert.Equal(t, 2*time.Second, c.retryWait())
}

func TestAppliedGTIDSetFoldsInFlight(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	s.logger = LogWithField("tag", "test")

	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetTarget) // :1-100
	applied, err := s.appliedGTIDSet()
	require.NoError(t, err)
	assert.Equal(t, gtidSetTarget, applied.String())

	s.inFlightGTID = "3e11fa47-71ca-11e1-9e33-c80aa9429562:101"
	applied, err = s.appliedGTIDSet()
	require.NoError(t, err)
	assert.Equal(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-101", applied.String())

	s.inFlightGTID = "not-a-gtid"
	_, err = s.appliedGTIDSet()
	assert.Error(t, err)
}

func TestGtidSetStringNil(t *testing.T) {
	assert.Equal(t, "", gtidSetString(nil))
	assert.Equal(t, gtidSetTarget, gtidSetString(mustParseGTID(t, gtidSetTarget)))
}

func TestUnionGTIDStringInto(t *testing.T) {
	res, err := unionGTIDStringInto(nil, "3e11fa47-71ca-11e1-9e33-c80aa9429562:101")
	require.NoError(t, err)
	assert.Equal(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:101", res.String())

	base := mustParseGTID(t, gtidSetTarget) // :1-100
	merged, err := unionGTIDStringInto(base, "3e11fa47-71ca-11e1-9e33-c80aa9429562:101")
	require.NoError(t, err)
	assert.Equal(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-101", merged.String())
	assert.Equal(t, gtidSetTarget, base.String(), "base must not be mutated")

	candidateMissing := mustParseGTID(t, gtidSetTarget)
	assert.False(t, candidateMissing.Contain(merged))
	candidateHas := mustParseGTID(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-101")
	assert.True(t, candidateHas.Contain(merged))
}

func TestUnionGTIDSets(t *testing.T) {
	res, err := unionGTIDSets(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "", res.String())

	applied := mustParseGTID(t, gtidSetTarget) // :1-100
	stop := mustParseGTID(t, gtidSetPast)      // :1-150
	merged, err := unionGTIDSets(applied, stop)
	require.NoError(t, err)
	assert.Equal(t, gtidSetPast, merged.String())
	assert.Equal(t, gtidSetTarget, applied.String())
	assert.Equal(t, gtidSetPast, stop.String())

	candidateShort := mustParseGTID(t, gtidSetTarget) // only :1-100
	assert.False(t, candidateShort.Contain(merged), "candidate missing stop-target GTIDs must fail")
}

// Unknown queries error, so the fixture cannot accidentally satisfy a query
// the validator should not be making.
type failoverServerFixture struct {
	readOnly int64
	gtidMode string
	executed string

	failQuery         string
	blockUntilCtxDone bool
}

var errFailoverFixtureQuery = errors.New("fixture query failed")

func (f failoverServerFixture) db(t *testing.T) *sql.DB {
	t.Helper()
	return newSourceRuntimeQueryDBWithQuery(t, func(ctx context.Context, query string) (driver.Rows, error) {
		if f.blockUntilCtxDone {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if f.failQuery != "" && query == f.failQuery {
			return nil, errFailoverFixtureQuery
		}
		switch query {
		case "SELECT @@read_only":
			return &sourceRuntimeQueryRows{columns: []string{"@@read_only"}, data: [][]driver.Value{{f.readOnly}}}, nil
		case "SELECT @@GLOBAL.gtid_mode":
			return &sourceRuntimeQueryRows{columns: []string{"@@GLOBAL.gtid_mode"}, data: [][]driver.Value{{f.gtidMode}}}, nil
		case "SELECT @@GLOBAL.GTID_EXECUTED":
			return &sourceRuntimeQueryRows{columns: []string{"@@GLOBAL.GTID_EXECUTED"}, data: [][]driver.Value{{f.executed}}}, nil
		}
		return nil, fmt.Errorf("unexpected fixture query %q", query)
	})
}

func TestValidateFailoverTarget(t *testing.T) {
	candidate := &DatabaseConfig{Host: "promoted", Port: 3306, User: "root", Net: "tcp"}
	writerAt100 := failoverServerFixture{readOnly: 0, gtidMode: "ON", executed: gtidSetTarget}

	t.Run("accepts a writer containing the required set", func(t *testing.T) {
		ctx := context.Background()
		require.NoError(t, validateFailoverTarget(ctx, writerAt100.db(t), candidate, mustParseGTID(t, gtidSetLower)))
		require.NoError(t, validateFailoverTarget(ctx, writerAt100.db(t), candidate, mustParseGTID(t, gtidSetTarget)))
	})

	t.Run("accepts an empty required set on a GTID-enabled server", func(t *testing.T) {
		fresh := failoverServerFixture{readOnly: 0, gtidMode: "ON", executed: ""}
		require.NoError(t, validateFailoverTarget(context.Background(), fresh.db(t), candidate, mustParseGTID(t, "")))
	})

	t.Run("rejects unsafe candidates", func(t *testing.T) {
		otherSID := "11111111-1111-1111-1111-111111111111:1"
		for _, tc := range []struct {
			name     string
			fixture  failoverServerFixture
			required mysql.GTIDSet
		}{
			{"read_only", failoverServerFixture{readOnly: 1, gtidMode: "ON", executed: gtidSetTarget}, mustParseGTID(t, gtidSetLower)},
			{"gtid mode off", failoverServerFixture{readOnly: 0, gtidMode: "OFF", executed: gtidSetTarget}, mustParseGTID(t, gtidSetLower)},
			{"malformed executed set", failoverServerFixture{readOnly: 0, gtidMode: "ON", executed: "not-a-gtid-set"}, mustParseGTID(t, gtidSetLower)},
			{"missing interval", writerAt100, mustParseGTID(t, gtidSetPast)},
			{"missing sid", writerAt100, mustParseGTID(t, otherSID)},
			{"read_only query error", failoverServerFixture{gtidMode: "ON", executed: gtidSetTarget, failQuery: "SELECT @@read_only"}, mustParseGTID(t, gtidSetLower)},
			{"executed set query error", failoverServerFixture{gtidMode: "ON", executed: gtidSetTarget, failQuery: "SELECT @@GLOBAL.GTID_EXECUTED"}, mustParseGTID(t, gtidSetLower)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				assert.Error(t, validateFailoverTarget(context.Background(), tc.fixture.db(t), candidate, tc.required))
			})
		}
	})

	t.Run("rejects missing inputs", func(t *testing.T) {
		ctx := context.Background()
		db := writerAt100.db(t)
		assert.Error(t, validateFailoverTarget(ctx, db, candidate, nil), "a nil required set is not a valid empty set")
		assert.Error(t, validateFailoverTarget(ctx, db, nil, mustParseGTID(t, gtidSetLower)))
		assert.Error(t, validateFailoverTarget(ctx, nil, candidate, mustParseGTID(t, gtidSetLower)))
	})

	t.Run("honours the caller's deadline", func(t *testing.T) {
		blocked := failoverServerFixture{blockUntilCtxDone: true}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := validateFailoverTarget(ctx, blocked.db(t), candidate, mustParseGTID(t, gtidSetLower))
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

// Uses the real publication path (ReplaceValidated with lazily opened handles)
// so rejected handles are really closed and published ones really retired.
type fakeReconnector struct {
	runtime *SourceRuntime

	// Handed out in order; the last one repeats.
	candidates []*DatabaseConfig
	executed   map[string]string
	resolveErr error

	calls         int
	callTimes     []time.Time
	previousSeen  []*DatabaseConfig
	requiredSeen  []mysql.GTIDSet
	finalRequired []mysql.GTIDSet
	// Every handle opened for a candidate, published or not.
	attempted []*sql.DB
	published []*sql.DB
}

func (f *fakeReconnector) candidateFor(call int) *DatabaseConfig {
	if call >= len(f.candidates) {
		return f.candidates[len(f.candidates)-1]
	}
	return f.candidates[call]
}

func (f *fakeReconnector) contains(host string, required mysql.GTIDSet) error {
	if required == nil {
		return errors.New("fake reconnector: nil required GTID set")
	}
	executed, err := mysql.ParseMysqlGTIDSet(f.executed[host])
	if err != nil {
		return err
	}
	if !executed.Contain(required) {
		return fmt.Errorf("fake reconnector: candidate %s executed %q does not contain required %q", host, executed.String(), required.String())
	}
	return nil
}

func (f *fakeReconnector) Reconnect(
	previous *DatabaseConfig,
	required mysql.GTIDSet,
	verifyStream func(*sql.DB, *DatabaseConfig) (mysql.GTIDSet, error),
) (*sql.DB, *DatabaseConfig, error) {
	call := f.calls
	f.calls++
	f.previousSeen = append(f.previousSeen, previous)
	f.requiredSeen = append(f.requiredSeen, required)
	f.callTimes = append(f.callTimes, time.Now())

	if verifyStream == nil {
		return nil, nil, errNilStreamVerifier
	}
	if f.resolveErr != nil {
		return nil, nil, f.resolveErr
	}
	if len(f.candidates) == 0 {
		return nil, nil, errors.New("fake reconnector: no candidates configured")
	}

	normalized, err := normalizeFailoverCandidate(f.candidateFor(call))
	if err != nil {
		return nil, nil, err
	}

	db, err := f.runtime.ReplaceValidated(normalized, nil, func(db *sql.DB) error {
		f.attempted = append(f.attempted, db)
		if err := f.contains(normalized.Host, required); err != nil {
			return err
		}

		final, err := verifyStream(db, normalized)
		if err != nil {
			return err
		}
		if final == nil {
			return errors.New("fake reconnector: nil final required GTID set")
		}
		f.finalRequired = append(f.finalRequired, final)

		if required.Contain(final) {
			return nil
		}
		// The trial raised the bar; revalidate before publishing.
		return f.contains(normalized.Host, final)
	})
	if err != nil {
		return nil, normalized, err
	}

	f.published = append(f.published, db)
	return db, normalized, nil
}
