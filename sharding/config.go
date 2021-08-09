package sharding

import (
	"github.com/Shopify/ghostferry"
)

type Config struct {
	*ghostferry.Config

	ShardingKey   string
	ShardingValue []int64
	SourceDB      string
	TargetDB      string

	SourceReplicationMaster       *ghostferry.DatabaseConfig
	ReplicatedMasterPositionQuery string
	RunFerryFromReplica           bool

	StatsDAddress string

	JoinedTables     map[string][]JoinTable
	IgnoredTables    []string
	PrimaryKeyTables []string

	Throttle *ghostferry.LagThrottlerConfig
}

func (c *Config) ValidateConfig() error {
	if c.RunFerryFromReplica && c.SourceReplicationMaster != nil {
		if err := c.SourceReplicationMaster.Validate(); err != nil {
			return err
		}
	}

	return c.Config.ValidateConfig()
}
