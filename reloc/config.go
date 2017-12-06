package reloc

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

	JoinedTables     map[string][]JoinTable
	IgnoredTables    []string
	PrimaryKeyTables []string

	Throttle *ghostferry.LagThrottlerConfig
}
