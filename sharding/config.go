package sharding

import (
	"fmt"

	"github.com/Shopify/ghostferry"
)

type Config struct {
	*ghostferry.Config

	ShardingKey   string
	ShardingValue int64
	SourceDB      string
	TargetDB      string

	SourceReplicationMaster       *ghostferry.DatabaseConfig
	ReplicatedMasterPositionQuery string
	RunFerryFromReplica           bool

	StatsDAddress string

	JoinedTables map[string][]JoinTable

	// IgnoredTables and IncludedTables are mutually exclusive. Specifying both is an error.
	IgnoredTables  []string
	IncludedTables []string

	PrimaryKeyTables []string

	Throttle *ghostferry.LagThrottlerConfig
}

func (c *Config) ValidateConfig() error {
	if len(c.IgnoredTables) != 0 && len(c.IncludedTables) != 0 {
		return fmt.Errorf("IgnoredTables and IncludedTables cannot be defined at the same time.")
	}

	if c.RunFerryFromReplica && c.SourceReplicationMaster != nil {
		if err := c.SourceReplicationMaster.Validate(); err != nil {
			return err
		}
	}

	return c.Config.ValidateConfig()
}
