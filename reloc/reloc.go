package reloc

import (
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type RelocFerry struct {
	Ferry    *ghostferry.Ferry
	verifier *ghostferry.IterativeVerifier
	config   *Config
	logger   *logrus.Entry
}

func NewFerry(config *Config) (*RelocFerry, error) {
	var err error

	config.DatabaseRewrites = map[string]string{config.SourceDB: config.TargetDB}

	primaryKeyTables := map[string]struct{}{}
	for _, name := range config.PrimaryKeyTables {
		primaryKeyTables[name] = struct{}{}
	}

	config.CopyFilter = &ShardedCopyFilter{
		ShardingKey:      config.ShardingKey,
		ShardingValue:    config.ShardingValue,
		JoinedTables:     config.JoinedTables,
		PrimaryKeyTables: primaryKeyTables,
	}

	ignored, err := compileRegexps(config.IgnoredTables)
	if err != nil {
		return nil, fmt.Errorf("failed to compile ignored tables: %v", err)
	}

	config.TableFilter = &ShardedTableFilter{
		ShardingKey:      config.ShardingKey,
		SourceShard:      config.SourceDB,
		JoinedTables:     config.JoinedTables,
		IgnoredTables:    ignored,
		PrimaryKeyTables: primaryKeyTables,
	}

	if err := config.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("failed to validate config: %v", err)
	}

	var throttler ghostferry.Throttler

	if config.Throttle != nil {
		throttler, err = ghostferry.NewLagThrottler(config.Throttle)
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

func (r *RelocFerry) Initialize() error {
	return r.Ferry.Initialize()
}

func (r *RelocFerry) Start() error {
	err := r.Ferry.Start()
	if err != nil {
		return err
	}

	verifierConcurrency := r.config.VerifierIterationConcurrency
	if verifierConcurrency == 0 {
		verifierConcurrency = r.config.DataIterationConcurrency
	}

	r.verifier = &ghostferry.IterativeVerifier{
		CursorConfig: &ghostferry.CursorConfig{
			DB:          r.Ferry.SourceDB,
			BatchSize:   r.config.DataIterationBatchSize,
			ReadRetries: r.config.DBReadRetries,
			BuildSelect: r.config.CopyFilter.BuildSelect,
		},

		BinlogStreamer: r.Ferry.BinlogStreamer,

		TableSchemaCache: r.Ferry.Tables,
		Tables:           r.Ferry.Tables.AsSlice(),

		SourceDB: r.Ferry.SourceDB,
		TargetDB: r.Ferry.TargetDB,

		DatabaseRewrites: r.config.DatabaseRewrites,
		TableRewrites:    r.config.TableRewrites,

		IgnoredTables: r.config.IgnoredVerificationTables,
		Concurrency:   verifierConcurrency,
	}

	return r.verifier.Initialize()
}

func (r *RelocFerry) Run() {
	copyWG := &sync.WaitGroup{}
	copyWG.Add(1)
	go func() {
		defer copyWG.Done()
		r.Ferry.Run()
	}()

	r.Ferry.WaitUntilRowCopyIsComplete()

	metrics.Measure("VerifyBeforeCutover", nil, 1.0, func() {
		err := r.verifier.VerifyBeforeCutover()
		if err != nil {
			r.logger.WithField("error", err).Errorf("pre-cutover verification encountered an error, aborting run")
			r.Ferry.ErrorHandler.Fatal("reloc", err)
		}
	})

	ghostferry.WaitForThrottle(r.Ferry.Throttler)

	r.Ferry.WaitUntilBinlogStreamerCatchesUp()

	var err error
	client := &http.Client{}

	cutoverStart := time.Now()
	// The callback must ensure that all in-flight transactions are complete and
	// there will be no more writes to the database after it returns.
	metrics.Measure("CutoverLock", nil, 1.0, func() {
		err = r.config.CutoverLock.Post(client)
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("locking failed, aborting run")
		r.Ferry.ErrorHandler.Fatal("reloc", err)
	}

	r.Ferry.Throttler.SetDisabled(true)

	r.Ferry.FlushBinlogAndStopStreaming()
	copyWG.Wait()

	metrics.Measure("deltaCopyJoinedTables", nil, 1.0, func() {
		err = r.deltaCopyJoinedTables()
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("failed to delta-copy joined tables after locking")
		r.Ferry.ErrorHandler.Fatal("reloc", err)
	}

	var verificationResult ghostferry.VerificationResult
	metrics.Measure("VerifyCutover", nil, 1.0, func() {
		verificationResult, err = r.verifier.VerifyDuringCutover()
	})
	if !verificationResult.DataCorrect {
		err = fmt.Errorf("verifier detected data discrepancy: %s", verificationResult.Message)
		r.logger.WithField("error", err).Errorf("verification failed, aborting run")
		r.Ferry.ErrorHandler.Fatal("reloc", err)
	} else if err != nil {
		r.logger.WithField("error", err).Errorf("verification encountered an error, aborting run")
		r.Ferry.ErrorHandler.Fatal("reloc", err)
	}

	r.Ferry.Throttler.SetDisabled(false)

	metrics.Measure("CutoverUnlock", nil, 1.0, func() {
		err = r.config.CutoverUnlock.Post(client)
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("unlocking failed, aborting run")
		r.Ferry.ErrorHandler.Fatal("reloc", err)
	}

	metrics.Timer("CutoverTime", time.Since(cutoverStart), nil, 1.0)
}

func (r *RelocFerry) deltaCopyJoinedTables() error {
	tables := []*schema.Table{}

	for _, table := range r.Ferry.Tables {
		if _, exists := r.config.JoinedTables[table.Name]; exists {
			tables = append(tables, table)
		}
	}

	return r.Ferry.RunStandaloneDataCopy(tables)
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
