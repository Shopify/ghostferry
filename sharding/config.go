package sharding

import (
	"github.com/Shopify/ghostferry"
)

type Config struct {
	*ghostferry.Config

	ShardingKey   string
	ShardingValue int64
	SourceDB      string
	TargetDB      string

	SourceReplicationMaster       ghostferry.DatabaseConfig
	ReplicatedMasterPositionQuery string
	RunFerryFromReplica           bool

	StatsDAddress string
	CutoverLock   ghostferry.HTTPCallback
	CutoverUnlock ghostferry.HTTPCallback
	ErrorCallback ghostferry.HTTPCallback

	JoinedTables     map[string][]JoinTable
	IgnoredTables    []string
	PrimaryKeyTables []string

	Throttle *ghostferry.LagThrottlerConfig

	// These two values configure the amount of times Ferry should attempt to
	// retry acquiring the cutover lock, and for how long the Ferry should wait
	// before attempting another lock acquisition
	MaxCutoverRetries       int
	CutoverRetryWaitSeconds int
}

func (c *Config) ValidateConfig() error {
	if c.MaxCutoverRetries == 0 {
		c.MaxCutoverRetries = 1
	}

	if c.CutoverRetryWaitSeconds == 0 {
		c.CutoverRetryWaitSeconds = 1
	}

	return c.Config.ValidateConfig()
}
