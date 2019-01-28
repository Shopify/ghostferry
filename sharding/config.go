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
}
