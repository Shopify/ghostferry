package reloc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"sync"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type RelocFerry struct {
	ferry  *ghostferry.Ferry
	config *Config
	logger *logrus.Entry
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
		config: config,
		logger: logrus.WithField("tag", "reloc"),
	}, nil
}

func (this *RelocFerry) Initialize() error {
	return this.ferry.Initialize()
}

func (this *RelocFerry) Start() error {
	return this.ferry.Start()
}

func (this *RelocFerry) Run() error {
	copyWG := &sync.WaitGroup{}
	copyWG.Add(1)
	go func() {
		defer copyWG.Done()
		this.ferry.Run()
	}()

	this.ferry.WaitUntilRowCopyIsComplete()

	this.ferry.WaitUntilBinlogStreamerCatchesUp()

	// The callback must ensure all writes are committed to the binlog by the time it returns.
	client := &http.Client{}
	lockErr := this.postCallback(client, this.config.CutoverLock.URI, map[string]interface{}{
		"Payload": this.config.CutoverLock.Payload,
	})

	this.ferry.FlushBinlogAndStopStreaming()
	copyWG.Wait()

	if lockErr != nil {
		this.logger.WithField("error", lockErr).Errorf("aborting unlock")
		return lockErr
	}

	unlockErr := this.postCallback(client, this.config.CutoverUnlock.URI, map[string]interface{}{
		"Payload": this.config.CutoverUnlock.Payload,
	})

	if unlockErr != nil {
		this.logger.WithField("error", unlockErr).Errorf("run failed")
		return unlockErr
	}

	return nil
}

func (this *RelocFerry) postCallback(client *http.Client, uri string, body interface{}) error {
	buf := bytes.Buffer{}

	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return err
	}

	logger := this.logger.WithField("uri", uri)
	logger.Infof("sending callback")

	res, err := client.Post(uri, "application/json", &buf)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		resBody, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logger.WithField("error", err).Errorf("error reading callback body")
		}
		logger.WithField("status", res.StatusCode).WithField("body", string(resBody)).Errorf("callback not ok")
		return fmt.Errorf("callback returned %s", res.Status)
	}

	return nil
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
