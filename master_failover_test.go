package ghostferry

import (
	"errors"
	"testing"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailoverRecoveryEnabled(t *testing.T) {
	reconnector := &fakeReconnector{}
	resolver := MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) {
		return nil, nil
	})
	_ = resolver

	// Disabled with no config.
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	assert.False(t, s.failoverRecoveryEnabled())

	// Config but no reconnector.
	s.MasterFailoverRecovery = &MasterFailoverRecoveryConfig{}
	assert.False(t, s.failoverRecoveryEnabled())

	// Config + reconnector but file/position mode.
	s.BinlogCoordinateMode = BinlogCoordinateFilePosition
	s.SourceReconnector = reconnector
	assert.False(t, s.failoverRecoveryEnabled())

	// GTID mode + config + reconnector: enabled.
	s.BinlogCoordinateMode = BinlogCoordinateGTID
	assert.True(t, s.failoverRecoveryEnabled())
}

func TestFailoverRetryWaitDefault(t *testing.T) {
	c := &MasterFailoverRecoveryConfig{}
	assert.Equal(t, DefaultFailoverRetryWait, c.retryWait())
	c.RetryWait = 2 * time.Second
	assert.Equal(t, 2*time.Second, c.retryWait())
}

func TestMasterWriterResolverFunc(t *testing.T) {
	want := &DatabaseConfig{Host: "new", Port: 3306}
	prev := &DatabaseConfig{Host: "old", Port: 3306}
	var got *DatabaseConfig
	resolver := MasterWriterResolverFunc(func(p *DatabaseConfig) (*DatabaseConfig, error) {
		got = p
		return want, nil
	})
	result, err := resolver.ResolveCurrentMaster(prev)
	require.NoError(t, err)
	assert.Same(t, want, result)
	assert.Same(t, prev, got)
}

func TestAppliedGTIDSetFoldsInFlight(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	s.logger = LogWithField("tag", "test")

	// Committed set only.
	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetTarget) // :1-100
	applied, err := s.appliedGTIDSet()
	require.NoError(t, err)
	assert.Equal(t, gtidSetTarget, applied.String())

	// With an in-flight GTID, the applied set extends to include it.
	s.inFlightGTID = "3e11fa47-71ca-11e1-9e33-c80aa9429562:101"
	applied, err = s.appliedGTIDSet()
	require.NoError(t, err)
	assert.Equal(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-101", applied.String())

	// A malformed in-flight GTID fails closed.
	s.inFlightGTID = "not-a-gtid"
	_, err = s.appliedGTIDSet()
	assert.Error(t, err)
}

func TestGtidSetStringNil(t *testing.T) {
	assert.Equal(t, "", gtidSetString(nil))
	assert.Equal(t, gtidSetTarget, gtidSetString(mustParseGTID(t, gtidSetTarget)))
}

func TestCloneOrEmpty(t *testing.T) {
	empty := cloneOrEmpty(nil)
	require.NotNil(t, empty)
	assert.Equal(t, "", empty.String())

	set := mustParseGTID(t, gtidSetTarget)
	clone := cloneOrEmpty(set)
	assert.Equal(t, gtidSetTarget, clone.String())
	require.NoError(t, clone.(*mysql.MysqlGTIDSet).Update("3e11fa47-71ca-11e1-9e33-c80aa9429562:101"))
	assert.Equal(t, gtidSetTarget, set.String(), "clone must not alias source")
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

	// The safety property: a candidate missing the in-flight GTID fails containment.
	candidateMissing := mustParseGTID(t, gtidSetTarget)
	assert.False(t, candidateMissing.Contain(merged))
	candidateHas := mustParseGTID(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-101")
	assert.True(t, candidateHas.Contain(merged))
}

func TestUnionGTIDSets(t *testing.T) {
	// nil inputs yield empty.
	res, err := unionGTIDSets(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "", res.String())

	applied := mustParseGTID(t, gtidSetTarget) // :1-100
	stop := mustParseGTID(t, gtidSetPast)      // :1-150
	merged, err := unionGTIDSets(applied, stop)
	require.NoError(t, err)
	assert.Equal(t, gtidSetPast, merged.String())
	// Inputs unchanged.
	assert.Equal(t, gtidSetTarget, applied.String())
	assert.Equal(t, gtidSetPast, stop.String())

	// A candidate must contain the union of applied + stop target.
	candidateShort := mustParseGTID(t, gtidSetTarget) // only :1-100
	assert.False(t, candidateShort.Contain(merged), "candidate missing stop-target GTIDs must fail")
}

// fakeReconnector is a test double for SourceReconnector.
type fakeReconnector struct {
	calls int
}

func (f *fakeReconnector) Reconnect(_ *DatabaseConfig, _ mysql.GTIDSet) (*sql.DB, *DatabaseConfig, error) {
	f.calls++
	return nil, nil, errors.New("not implemented")
}
