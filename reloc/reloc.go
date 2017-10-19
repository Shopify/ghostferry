package reloc

import (
	"sync"

	"github.com/Shopify/ghostferry"
)

type RelocFerry struct {
	ferry *ghostferry.Ferry
}

func NewFerry(shardingKey string, shardingValue interface{}, sourceShardDb string, config *ghostferry.Config) *RelocFerry {
	config.CopyFilter = &ShardedRowFilter{
		ShardingKey:   shardingKey,
		ShardingValue: shardingValue,
	}

	config.TableFilter = &ShardedTableFilter{
		ShardingKey: shardingKey,
		SourceShard: sourceShardDb,
	}

	return &RelocFerry{
		ferry: &ghostferry.Ferry{Config: config},
	}
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
