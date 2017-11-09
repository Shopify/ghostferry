package reloc

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/Shopify/ghostferry"
)

type RelocFerry struct {
	ferry *ghostferry.Ferry
}

func NewFerry(config *Config) (*RelocFerry, error) {
	var err error
	config.DatabaseTargets = map[string]string{config.SourceDB: config.TargetDB}

	config.CopyFilter = &ShardedRowFilter{
		ShardingKey:   config.ShardingKey,
		ShardingValue: config.ShardingValue,
		JoinedTables:  config.JoinedTables,
	}

	ignored, err := compileRegexps(config.IgnoredTables)
	if err != nil {
		return nil, fmt.Errorf("failed to compile ignored tables: %v", err)
	}

	config.TableFilter = &ShardedTableFilter{
		ShardingKey:   config.ShardingKey,
		SourceShard:   config.SourceDB,
		JoinedTables:  config.JoinedTables,
		IgnoredTables: ignored,
	}

	if err := config.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("failed to validate config: %v", err)
	}

	var throttler ghostferry.Throttler

	if config.Throttle != nil {
		throttler, err = NewLagThrottler(config.Throttle)
		if err != nil {
			return nil, fmt.Errorf("failed to create throttler: %v", err)
		}
	}

	return &RelocFerry{
		ferry: &ghostferry.Ferry{
			Config:    &config.Config,
			Throttler: throttler,
		},
	}, nil
}

func (this *RelocFerry) Initialize() error {
	return this.ferry.Initialize()
}

func (this *RelocFerry) Start() error {
	return this.ferry.Start()
}

func (this *RelocFerry) Run() {
	copyWG := &sync.WaitGroup{}
	copyWG.Add(1)
	go func() {
		defer copyWG.Done()
		this.ferry.Run()
	}()

	this.ferry.WaitUntilRowCopyIsComplete()

	this.ferry.WaitUntilBinlogStreamerCatchesUp()

	// Call cutover lock callback here.
	// The callback must ensure all writes are committed to the binlog by the time it returns.

	this.ferry.FlushBinlogAndStopStreaming()
	copyWG.Wait()

	// Source and target are identical.
	// Call cutover unlock callback here.
}

func compileRegexps(exps []string) ([]*regexp.Regexp, error) {
	var err error
	res := make([]*regexp.Regexp, len(exps))

	for i, exp := range exps {
		res[i], err = regexp.Compile(exp)

		if err != nil {
			return nil, err
		}
	}

	return res, nil
}
