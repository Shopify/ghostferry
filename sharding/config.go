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

	StatsDAddress string
	CutoverLock   HTTPCallback
	CutoverUnlock HTTPCallback
	ErrorCallback HTTPCallback

	JoinedTables              map[string][]JoinTable
	IgnoredTables             []string
	IgnoredVerificationTables []string
	PrimaryKeyTables          []string

	VerifierIterationConcurrency int

	Throttle *ghostferry.LagThrottlerConfig
}
