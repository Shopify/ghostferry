package reloc

import (
	"fmt"
	"net/http"
	"regexp"
	"sync"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type RelocFerry struct {
	Ferry  *ghostferry.Ferry
	config *Config
	logger *logrus.Entry
}

func NewFerry(config *Config) (*RelocFerry, error) {
	var err error
	config.DatabaseRenames = map[string]string{config.SourceDB: config.TargetDB}

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
		Ferry: &ghostferry.Ferry{
			Config:    config.Config,
			Throttler: throttler,
		},
		config: config,
		logger: logrus.WithField("tag", "reloc"),
	}, nil
}

func (this *RelocFerry) Initialize() error {
	return this.Ferry.Initialize()
}

func (this *RelocFerry) Start() error {
	return this.Ferry.Start()
}

func (this *RelocFerry) Run() {
	copyWG := &sync.WaitGroup{}
	copyWG.Add(1)
	go func() {
		defer copyWG.Done()
		this.Ferry.Run()
	}()

	this.Ferry.WaitUntilRowCopyIsComplete()

	this.Ferry.WaitUntilBinlogStreamerCatchesUp()

	// The callback must ensure that all in-flight transactions are complete and
	// there will be no more writes to the database after it returns.
	client := &http.Client{}
	err := this.config.CutoverLock.Post(client)
	if err != nil {
		this.logger.WithField("error", err).Errorf("locking failed, aborting run")
		this.Ferry.ErrorHandler.Fatal("reloc", err)
	}

	this.Ferry.FlushBinlogAndStopStreaming()
	copyWG.Wait()

	err = this.config.CutoverUnlock.Post(client)
	if err != nil {
		this.logger.WithField("error", err).Errorf("unlocking failed, aborting run")
		this.Ferry.ErrorHandler.Fatal("reloc", err)
	}
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
