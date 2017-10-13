package reloc

import (
	"sync"

	"github.com/Shopify/ghostferry"
)

type RelocFerry struct {
	ferry  *ghostferry.Ferry
	filter *ShardingFilter
}

func NewFerry(shardingKey string, shardingValue interface{}, sourceShardDb string, config *ghostferry.Config) *RelocFerry {
	filter := &ShardingFilter{
		ShardingKey:   shardingKey,
		ShardingValue: shardingValue,
	}

	config.Applicability = &ShardedApplicableFilter{
		ShardingKey: shardingKey,
		SourceShard: sourceShardDb,
	}

	ferry := &ghostferry.Ferry{
		Config: config,
		Filter: filter,
	}

	return &RelocFerry{
		ferry:  ferry,
		filter: filter,
	}
}

func (this *RelocFerry) Initialize() error {
	err := this.ferry.Initialize()
	if err != nil {
		return err
	}
	return nil
}

func (this *RelocFerry) Start() error {
	err := this.ferry.Start()
	if err != nil {
		return err
	}
	return nil
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
