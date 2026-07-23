package ghostferry

import (
	"errors"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailoverRecoveryEnabled(t *testing.T) {
	resolver := MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) {
		return nil, nil
	})

	// Disabled when no config.
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	assert.False(t, s.failoverRecoveryEnabled())

	// Disabled when config present but no resolver.
	s.MasterFailoverRecovery = &MasterFailoverRecoveryConfig{}
	assert.False(t, s.failoverRecoveryEnabled())

	// Disabled in file/position mode even with a resolver.
	s.BinlogCoordinateMode = BinlogCoordinateFilePosition
	s.MasterFailoverRecovery = &MasterFailoverRecoveryConfig{Resolver: resolver}
	assert.False(t, s.failoverRecoveryEnabled())

	// Enabled only in GTID mode with a resolver.
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
	want := &DatabaseConfig{Host: "new-master", Port: 3306}
	prev := &DatabaseConfig{Host: "old-master", Port: 3306}

	var got *DatabaseConfig
	resolver := MasterWriterResolverFunc(func(p *DatabaseConfig) (*DatabaseConfig, error) {
		got = p
		return want, nil
	})

	result, err := resolver.ResolveCurrentMaster(prev)
	assert.NoError(t, err)
	assert.Equal(t, want, result)
	assert.Equal(t, prev, got, "resolver should receive the previous config")

	errResolver := MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) {
		return nil, errors.New("boom")
	})
	_, err = errResolver.ResolveCurrentMaster(prev)
	assert.Error(t, err)
}

func TestGTIDSetStringNil(t *testing.T) {
	assert.Equal(t, "", gtidSetString(nil))
	set := mustParseGTID(t, gtidSetTarget)
	assert.Equal(t, gtidSetTarget, gtidSetString(set))
}

func TestCloneOrEmpty(t *testing.T) {
	// Nil yields a fresh empty set, not nil.
	empty := cloneOrEmpty(nil)
	assert.NotNil(t, empty)
	assert.Equal(t, "", empty.String())

	set := mustParseGTID(t, gtidSetTarget)
	clone := cloneOrEmpty(set)
	assert.Equal(t, gtidSetTarget, clone.String())
	// Mutating the clone must not affect the original.
	require.NoError(t, clone.(*mysql.MysqlGTIDSet).Update("3e11fa47-71ca-11e1-9e33-c80aa9429562:101"))
	assert.Equal(t, gtidSetTarget, set.String(), "clone must not alias the source")
}

func TestUnionGTIDStringInto(t *testing.T) {
	// nil base + a GTID yields just that GTID.
	res, err := unionGTIDStringInto(nil, "3e11fa47-71ca-11e1-9e33-c80aa9429562:101")
	require.NoError(t, err)
	assert.Equal(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:101", res.String())

	// Folding an in-flight GTID into a committed set extends the range and does
	// not mutate the base.
	base := mustParseGTID(t, gtidSetTarget) // :1-100
	merged, err := unionGTIDStringInto(base, "3e11fa47-71ca-11e1-9e33-c80aa9429562:101")
	require.NoError(t, err)
	assert.Equal(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-101", merged.String())
	assert.Equal(t, gtidSetTarget, base.String(), "base must not be mutated")

	// A candidate missing the in-flight GTID must NOT contain the union: this is
	// the safety property failover validation relies on.
	candidateMissing := mustParseGTID(t, gtidSetTarget) // has :1-100 only
	assert.False(t, candidateMissing.Contain(merged), "candidate missing in-flight GTID must fail containment")

	// A candidate that has the in-flight GTID does contain the union.
	candidateHas := mustParseGTID(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-101")
	assert.True(t, candidateHas.Contain(merged))
}
