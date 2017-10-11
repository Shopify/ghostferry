package reloc

import (
	"sync"

	"github.com/Shopify/ghostferry"
)

type RelocFerry struct {
	ferry *ghostferry.Ferry
}

func NewFerry(shardingKey string, shardingValue interface{}, config *ghostferry.Config) *RelocFerry {
	filter := &ShardingFilter{
		ShardingKey:   shardingKey,
		ShardingValue: shardingValue,
	}

	ferry := &ghostferry.Ferry{
		Config: config,
		Filter: filter,
	}

	return &RelocFerry{ferry: ferry}
}

func (this *RelocFerry) Initialize() error {
	return this.ferry.Initialize()
}

func (this *RelocFerry) Start() error {
	err := this.ferry.Start()
	if err != nil {
		return err
	}
	return nil
}

func (this *RelocFerry) Run() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		this.ferry.Run()
	}()

	this.ferry.WaitUntilRowCopyIsComplete()
	this.ferry.WaitUntilBinlogStreamerCatchesUp()

	// Call cutover lock callback here.

	this.ferry.FlushBinlogAndStopStreaming()
	wg.Wait()

	// Source and target are identical.
	// Call cutover unlock callback here.
}
