package sharding

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

type ShardingFerry struct {
	Ferry    *ghostferry.Ferry
	verifier *ghostferry.IterativeVerifier
	config   *Config
	logger   *logrus.Entry
}

func NewFerry(config *Config) (*ShardingFerry, error) {
	var err error

	config.DatabaseRewrites = map[string]string{config.SourceDB: config.TargetDB}

	config.CopyFilter = &ShardedCopyFilter{
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
		throttler, err = ghostferry.NewLagThrottler(config.Throttle)
		if err != nil {
			return nil, fmt.Errorf("failed to create throttler: %v", err)
		}
	}

	ferry := &ghostferry.Ferry{
		Config:    config.Config,
		Throttler: throttler,
	}

	logger := logrus.WithField("tag", "sharding")

	ferry.ErrorHandler = &ShardingErrorHandler{
		ErrorHandler: &ghostferry.PanicErrorHandler{
			Ferry: ferry,
		},
		ErrorCallback: config.ErrorCallback,
		Logger:        logger,
	}

	return &ShardingFerry{
		Ferry:  ferry,
		config: config,
		logger: logger,
	}, nil
}

func (r *ShardingFerry) Initialize() error {
	if r.config.RunFerryFromReplica {
		err := r.initializeWaitUntilReplicaIsCaughtUpToMasterConnection()
		if err != nil {
			r.logger.WithField("error", err).Errorf("could not open connection to master replica")
			return err
		}
	}

	return r.Ferry.Initialize()
}

func (r *ShardingFerry) newIterativeVerifier() (*ghostferry.IterativeVerifier, error) {
	verifierConcurrency := r.config.VerifierIterationConcurrency
	if verifierConcurrency == 0 {
		verifierConcurrency = r.config.DataIterationConcurrency
	}

	var maxExpectedDowntime time.Duration
	if r.config.MaxExpectedVerifierDowntime != "" {
		var err error
		maxExpectedDowntime, err = time.ParseDuration(r.config.MaxExpectedVerifierDowntime)
		if err != nil {
			return nil, fmt.Errorf("invalid MaxExpectedVerifierDowntime: %s", err)
		}
	}

	return &ghostferry.IterativeVerifier{
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

		IgnoredTables:       r.config.IgnoredVerificationTables,
		Concurrency:         verifierConcurrency,
		MaxExpectedDowntime: maxExpectedDowntime,
	}, nil
}

func (r *ShardingFerry) Start() error {
	err := r.Ferry.Start()
	if err != nil {
		return err
	}

	r.verifier, err = r.newIterativeVerifier()
	if err != nil {
		return err
	}

	return r.verifier.Initialize()
}

func (r *ShardingFerry) Run() {
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
			r.Ferry.ErrorHandler.Fatal("sharding", err)
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
		r.Ferry.ErrorHandler.Fatal("sharding", err)
	}

	r.Ferry.Throttler.SetDisabled(true)

	r.Ferry.FlushBinlogAndStopStreaming()
	copyWG.Wait()

	metrics.Measure("deltaCopyJoinedTables", nil, 1.0, func() {
		err = r.deltaCopyJoinedTables()
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("failed to delta-copy joined tables after locking")
		r.Ferry.ErrorHandler.Fatal("sharding", err)
	}

	var verificationResult ghostferry.VerificationResult
	metrics.Measure("VerifyCutover", nil, 1.0, func() {
		verificationResult, err = r.verifier.VerifyDuringCutover()
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("verification encountered an error, aborting run")
		r.Ferry.ErrorHandler.Fatal("iterative_verifier", err)
	} else if !verificationResult.DataCorrect {
		err = fmt.Errorf("verifier detected data discrepancy: %s", verificationResult.Message)
		r.logger.WithField("error", err).Errorf("verification failed, aborting run")
		r.Ferry.ErrorHandler.Fatal("iterative_verifier", err)
	}

	metrics.Measure("CopyPrimaryKeyTables", nil, 1.0, func() {
		err = r.copyPrimaryKeyTables()
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("copying primary key table failed")
		r.Ferry.ErrorHandler.Fatal("sharding", err)
	}

	r.Ferry.Throttler.SetDisabled(false)

	metrics.Measure("CutoverUnlock", nil, 1.0, func() {
		err = r.config.CutoverUnlock.Post(client)
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("unlocking failed, aborting run")
		r.Ferry.ErrorHandler.Fatal("sharding", err)
	}

	metrics.Timer("CutoverTime", time.Since(cutoverStart), nil, 1.0)
}

func (r *ShardingFerry) deltaCopyJoinedTables() error {
	tables := []*schema.Table{}

	for _, table := range r.Ferry.Tables {
		if _, exists := r.config.JoinedTables[table.Name]; exists {
			tables = append(tables, table)
		}
	}

	return r.Ferry.RunStandaloneDataCopy(tables)
}

func (r *ShardingFerry) copyPrimaryKeyTables() error {
	if len(r.config.PrimaryKeyTables) == 0 {
		return nil
	}

	pkTables := map[string]struct{}{}
	for _, name := range r.config.PrimaryKeyTables {
		pkTables[name] = struct{}{}
	}

	r.config.TableFilter.(*ShardedTableFilter).PrimaryKeyTables = pkTables
	r.config.CopyFilter.(*ShardedCopyFilter).PrimaryKeyTables = pkTables

	sourceDbTables, err := ghostferry.LoadTables(r.Ferry.SourceDB, r.config.TableFilter)
	if err != nil {
		return err
	}

	tables := []*schema.Table{}
	for _, table := range sourceDbTables.AsSlice() {
		if _, exists := pkTables[table.Name]; exists {
			if len(table.PKColumns) != 1 {
				return fmt.Errorf("Multiple PK columns are not supported with the PrimaryKeyTables option")
			}
			tables = append(tables, table)
		}
	}

	if len(tables) == 0 {
		return fmt.Errorf("expected primary key tables could not be found")
	}

	err = r.Ferry.RunStandaloneDataCopy(tables)
	if err != nil {
		return err
	}

	verifier, err := r.newIterativeVerifier()
	if err != nil {
		return err
	}

	verifier.TableSchemaCache = sourceDbTables
	verifier.Tables = tables

	err = verifier.Initialize()
	if err != nil {
		return err
	}

	verificationResult, err := verifier.VerifyOnce()
	if err != nil {
		return err
	} else if !verificationResult.DataCorrect {
		return fmt.Errorf("primary key tables verifier detected data discrepancy: %s", verificationResult.Message)
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

func (r *ShardingFerry) initializeWaitUntilReplicaIsCaughtUpToMasterConnection() error {
	masterDB, err := r.config.SourceReplicationMaster.SqlDB(r.logger)
	if err != nil {
		return err
	}

	positionFetcher := ghostferry.ReplicatedMasterPositionViaCustomQuery{Query: r.config.ReplicatedMasterPositionQuery}

	r.Ferry.WaitUntilReplicaIsCaughtUpToMaster = &ghostferry.WaitUntilReplicaIsCaughtUpToMaster{
		MasterDB:                        masterDB,
		ReplicatedMasterPositionFetcher: positionFetcher,
	}
	return nil
}

func (r *ShardingFerry) dbConfigIsForReplica(dbConfig ghostferry.DatabaseConfig) (bool, error) {
	conn, err := dbConfig.SqlDB(r.logger)
	defer conn.Close()
	if err != nil {
		return false, err
	}

	row := conn.QueryRow("SELECT @@read_only")

	var isReadOnly bool
	err = row.Scan(&isReadOnly)

	return isReadOnly, err
}
