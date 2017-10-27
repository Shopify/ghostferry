package reloc

import (
	"fmt"
	"sync"

	"github.com/Shopify/ghostferry"
)

type RelocFerry struct {
	ferry *ghostferry.Ferry
}

func NewFerry(config *Config) (*RelocFerry, error) {
	config.DatabaseTargets = map[string]string{config.SourceDB: config.TargetDB}

	config.CopyFilter = &ShardedRowFilter{
		ShardingKey:   config.ShardingKey,
		ShardingValue: config.ShardingValue,
		JoinedTables:  config.JoinedTables,
	}

	config.TableFilter = &ShardedTableFilter{
		ShardingKey:  config.ShardingKey,
		SourceShard:  config.SourceDB,
		JoinedTables: config.JoinedTables,
	}

	if err := config.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("failed to validate config: %v", err)
	}

	return &RelocFerry{
		ferry: &ghostferry.Ferry{Config: &config.Config},
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
