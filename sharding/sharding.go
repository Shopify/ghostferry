package sharding

import (
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type ShardingFerry struct {
	Ferry  *ghostferry.Ferry
	config *Config
	logger *logrus.Entry
}

func NewFerry(config *Config) (*ShardingFerry, error) {
	var err error

	config.DatabaseRewrites = map[string]string{config.SourceDB: config.TargetDB}

	config.CopyFilter = &ShardedCopyFilter{
		ShardingKey:   config.ShardingKey,
		ShardingValue: config.ShardingValue,
		JoinedTables:  config.JoinedTables,
	}

	config.VerifierType = ghostferry.VerifierTypeInline

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

	ferry.ErrorHandler = &ghostferry.PanicErrorHandler{
		Ferry:         ferry,
		ErrorCallback: config.ErrorCallback,
		DumpState:     false,
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

	err := r.Ferry.Initialize()
	if err != nil {
		r.Ferry.ErrorHandler.ReportError("ferry.initialize", err)
		return err
	}

	return nil
}

func (r *ShardingFerry) Start() error {
	return r.Ferry.Start()
}

func (r *ShardingFerry) Run() {
	copyWG := &sync.WaitGroup{}
	copyWG.Add(1)
	go func() {
		defer copyWG.Done()
		r.Ferry.Run()
	}()

	r.Ferry.WaitUntilRowCopyIsComplete()

	ghostferry.WaitForThrottle(r.Ferry.Throttler)

	r.Ferry.WaitUntilBinlogStreamerCatchesUp()

	r.AbortIfTargetDbNoLongerWriteable()

	var (
		err          error
		client       http.Client
		cutoverStart time.Time
	)

	// The callback must ensure that all in-flight transactions are complete and
	// there will be no more writes to the database after it returns.
	err = ghostferry.WithRetries(r.config.MaxCutoverRetries, time.Duration(r.config.CutoverRetryWaitSeconds)*time.Second, r.logger, "get cutover lock", func() (err error) {
		metrics.Measure("CutoverLock", nil, 1.0, func() {
			cutoverStart = time.Now()
			err = r.config.CutoverLock.Post(&client)
		})
		return err
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("locking failed, aborting run")
		r.Ferry.ErrorHandler.Fatal("sharding", err)
	}

	r.Ferry.Throttler.SetDisabled(true)

	r.Ferry.FlushBinlogAndStopStreaming()
	copyWG.Wait()

	// Joined tables cannot be easily monitored for changes in the binlog stream
	// so we copy and verify them for a second time during the cutover phase to
	// pick up any new changes since DataIterator copied the tables
	metrics.Measure("deltaCopyJoinedTables", nil, 1.0, func() {
		err = r.deltaCopyJoinedTables()
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("failed to delta-copy joined tables after locking")
		r.Ferry.ErrorHandler.Fatal("sharding.delta_copy", err)
	}

	var verificationResult ghostferry.VerificationResult
	metrics.Measure("VerifyCutover", nil, 1.0, func() {
		verificationResult, err = r.Ferry.Verifier.VerifyDuringCutover()
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("verification encountered an error, aborting run")
		r.Ferry.ErrorHandler.Fatal("inline_verifier", err)
	} else if !verificationResult.DataCorrect {
		err = fmt.Errorf("verifier detected data discrepancy: %s", verificationResult.Message)
		r.logger.WithField("error", err).Errorf("verification failed, aborting run")
		r.Ferry.ErrorHandler.Fatal("inline_verifier", err)
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
		err = r.config.CutoverUnlock.Post(&client)
	})
	if err != nil {
		r.logger.WithField("error", err).Errorf("unlocking failed, aborting run")
		r.Ferry.ErrorHandler.Fatal("sharding", err)
	}

	metrics.Timer("CutoverTime", time.Since(cutoverStart), nil, 1.0)
}

func (r *ShardingFerry) deltaCopyJoinedTables() error {
	tables := []*ghostferry.TableSchema{}

	for _, table := range r.Ferry.Tables {
		if _, exists := r.config.JoinedTables[table.Name]; exists {
			tables = append(tables, table)
		}
	}

	err := r.Ferry.RunStandaloneDataCopy(tables)
	if err != nil {
		return err
	}

	return nil
}

func (r *ShardingFerry) copyPrimaryKeyTables() error {
	if len(r.config.PrimaryKeyTables) == 0 {
		return nil
	}

	primaryKeyTables := map[string]struct{}{}
	for _, name := range r.config.PrimaryKeyTables {
		primaryKeyTables[name] = struct{}{}
	}

	r.config.TableFilter.(*ShardedTableFilter).PrimaryKeyTables = primaryKeyTables
	r.config.CopyFilter.(*ShardedCopyFilter).PrimaryKeyTables = primaryKeyTables

	sourceDbTables, err := ghostferry.LoadTables(r.Ferry.SourceDB, r.config.TableFilter, r.config.CompressedColumnsForVerification, r.config.IgnoredColumnsForVerification, r.config.CascadingPaginationColumnConfig)
	if err != nil {
		return err
	}

	tables := []*ghostferry.TableSchema{}
	for _, table := range sourceDbTables.AsSlice() {
		if _, exists := primaryKeyTables[table.Name]; exists {
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

func (r *ShardingFerry) dbConfigIsForReplica(dbConfig *ghostferry.DatabaseConfig) (bool, error) {
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

func (r *ShardingFerry) AbortIfTargetDbNoLongerWriteable() {
	isReplica, err := ghostferry.CheckDbIsAReplica(r.Ferry.TargetDB)
	if err != nil {
		r.logger.WithError(err).Error("cannot check if target db is writable")
		r.Ferry.ErrorHandler.Fatal("sharding", err)
	}
	if isReplica {
		r.Ferry.ErrorHandler.Fatal("sharding", fmt.Errorf("@@read_only must be OFF on target db"))
	}
}
