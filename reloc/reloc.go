package reloc

import (
	"sync"

	"github.com/Shopify/ghostferry"
)

type RelocFerry struct {
	ferry         *ghostferry.Ferry
	filter        *ShardingFilter
	controlServer *ghostferry.ControlServer
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

	controlServer := &ghostferry.ControlServer{
		F:       ferry,
		Addr:    config.ServerBindAddr,
		Basedir: config.WebBasedir,
	}

	return &RelocFerry{
		ferry:         ferry,
		filter:        filter,
		controlServer: controlServer,
	}
}

func (this *RelocFerry) Initialize() error {
	err := this.ferry.Initialize()
	if err != nil {
		return err
	}

	return this.controlServer.Initialize()
}

func (this *RelocFerry) Start() error {
	err := this.ferry.Start()
	if err != nil {
		return err
	}
	return nil
}

func (this *RelocFerry) Run() {
	serverWG := &sync.WaitGroup{}
	serverWG.Add(1)
	go this.controlServer.Run(serverWG)

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

	// Work is done, the process will run the web server until killed.
	serverWG.Wait()
}
