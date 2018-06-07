package sharding

import (
	"time"

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
	MaxExpectedVerifierDowntime  time.Duration

	Throttle *ghostferry.LagThrottlerConfig

	DryRunVerification bool
}
