package ghostferry

import (
	"errors"
	"testing"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func reconnectorTestConfig(host string) *DatabaseConfig {
	return &DatabaseConfig{Host: host, Port: 3306, User: "root", Net: "tcp"}
}

func TestNewSourceReconnectorNilWhenDisabled(t *testing.T) {
	f := &Ferry{Config: &Config{}}
	assert.Nil(t, f.newSourceReconnector())

	f.Config.MasterFailoverRecovery = &MasterFailoverRecoveryConfig{}
	assert.Nil(t, f.newSourceReconnector(), "nil resolver disables reconnector")
}

func TestReconnectPropagatesResolverError(t *testing.T) {
	wantErr := errors.New("no master found")
	f := &Ferry{
		Config:        &Config{Source: reconnectorTestConfig("old")},
		logger:        LogWithField("tag", "test"),
		sourceRuntime: NewSourceRuntime(&sql.DB{Marginalia: "old"}, reconnectorTestConfig("old")),
	}
	r := &ferrySourceReconnector{
		ferry:    f,
		resolver: MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) { return nil, wantErr }),
	}

	_, _, err := r.Reconnect(reconnectorTestConfig("old"), nil)
	assert.ErrorIs(t, err, wantErr)
}

func TestReconnectRejectsNilCandidate(t *testing.T) {
	f := &Ferry{
		Config:        &Config{Source: reconnectorTestConfig("old")},
		logger:        LogWithField("tag", "test"),
		sourceRuntime: NewSourceRuntime(&sql.DB{Marginalia: "old"}, reconnectorTestConfig("old")),
	}
	r := &ferrySourceReconnector{
		ferry:    f,
		resolver: MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) { return nil, nil }),
	}

	_, _, err := r.Reconnect(reconnectorTestConfig("old"), nil)
	assert.ErrorIs(t, err, errNilCandidate)

	// The runtime must not have been swapped when the candidate is nil.
	assert.Equal(t, "old", f.sourceRuntime.Config().Host)
}

func TestNewSourceReconnectorWiredWhenConfigured(t *testing.T) {
	f := &Ferry{
		Config: &Config{
			MasterFailoverRecovery: &MasterFailoverRecoveryConfig{
				Resolver: MasterWriterResolverFunc(func(_ *DatabaseConfig) (*DatabaseConfig, error) { return nil, nil }),
			},
		},
	}
	r := f.newSourceReconnector()
	require.NotNil(t, r)
	_, ok := r.(*ferrySourceReconnector)
	assert.True(t, ok)
}
