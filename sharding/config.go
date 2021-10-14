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

	StatsdAddress   string
	StatsdQueueSize int64

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

func (c *Config) InitializeAndValidateConfig() error {
	if c.MyServerId != 0 {
		return fmt.Errorf("specifying MyServerId option manually is dangerous and disallowed")
	}

	if c.ShardingKey == "" {
		return fmt.Errorf("missing ShardingKey config")
	}

	if c.ShardingValue == -1 {
		return fmt.Errorf("missing ShardingValue config")
	}

	if c.SourceDB == "" {
		return fmt.Errorf("missing SourceDB config")
	}

	if c.TargetDB == "" {
		return fmt.Errorf("missing TargetDB config")
	}

	if c.StatsdQueueSize == 0 {
		c.StatsdQueueSize = 4096
	}

	return nil
}
