package reloc

import (
	"fmt"
	"sync"

	"github.com/Shopify/ghostferry"
)

type RelocFerry struct {
	ferry *ghostferry.Ferry
}

func NewFerry(shardingKey string, shardingValue interface{}, sourceDb, targetDb string, config *ghostferry.Config) (*RelocFerry, error) {
	config.DatabaseTargets = map[string]string{sourceDb: targetDb}

	config.CopyFilter = &ShardedRowFilter{
		ShardingKey:   shardingKey,
		ShardingValue: shardingValue,
	}

	config.TableFilter = &ShardedTableFilter{
		ShardingKey: shardingKey,
		SourceShard: sourceDb,
	}

	if err := config.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("failed to validate config: %v", err)
	}

	return &RelocFerry{
		ferry: &ghostferry.Ferry{Config: config},
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
