package ghostferry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	siddontanglog "github.com/siddontang/go-log/log"
	siddontangmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

var (
	VersionString string = "?.?.?+??????????????+???????"
	WebUiBasedir  string = ""
)

const (
	StateStarting            = "starting"
	StateCopying             = "copying"
	StateWaitingForCutover   = "wait-for-cutover"
	StateVerifyBeforeCutover = "verify-before-cutover"
	StateCutover             = "cutover"
	StateDone                = "done"
)

func quoteField(field string) string {
	return fmt.Sprintf("`%s`", field)
}

func MaskedDSN(c *mysql.Config) string {
	oldPass := c.Passwd
	c.Passwd = "<masked>"
	defer func() {
		c.Passwd = oldPass
	}()

	return c.FormatDSN()
}

type Ferry struct {
	*Config

	SourceDB *sql.DB
	TargetDB *sql.DB

	BinlogStreamer *BinlogStreamer
	BinlogWriter   *BinlogWriter

	DataIterator *DataIterator
	BatchWriter  *BatchWriter

	StateTracker                       *StateTracker
	ErrorHandler                       ErrorHandler
	Throttler                          Throttler
	WaitUntilReplicaIsCaughtUpToMaster *WaitUntilReplicaIsCaughtUpToMaster

	// This can be specified by the caller. If specified, do not specify
	// VerifierType in Config (or as an empty string) or an error will be
	// returned in Initialize.
	//
	// If VerifierType is specified and this is nil on Ferry initialization, a
	// Verifier will be created by Initialize. If an IterativeVerifier is to be
	// created, IterativeVerifierConfig will be used to create the verifier.
	Verifier       Verifier
	inlineVerifier *InlineVerifier

	Tables TableSchemaCache

	StartTime    time.Time
	DoneTime     time.Time
	OverallState string

	logger *logrus.Entry

	rowCopyCompleteCh chan struct{}
}

func (f *Ferry) NewDataIterator() *DataIterator {
	f.ensureInitialized()

	dataIterator := &DataIterator{
		DB:                f.SourceDB,
		Concurrency:       f.Config.DataIterationConcurrency,
		SelectFingerprint: f.Config.VerifierType == VerifierTypeInline,

		ErrorHandler: f.ErrorHandler,
		CursorConfig: &CursorConfig{
			DB:        f.SourceDB,
			Throttler: f.Throttler,

			BatchSize:   f.Config.DataIterationBatchSize,
			ReadRetries: f.Config.DBReadRetries,
		},
		StateTracker: f.StateTracker,
	}

	if f.CopyFilter != nil {
		dataIterator.CursorConfig.BuildSelect = f.CopyFilter.BuildSelect
	}

	return dataIterator
}

func (f *Ferry) NewDataIteratorWithoutStateTracker() *DataIterator {
	dataIterator := f.NewDataIterator()
	dataIterator.StateTracker = nil
	return dataIterator
}

func (f *Ferry) NewBinlogStreamer() *BinlogStreamer {
	f.ensureInitialized()

	return &BinlogStreamer{
		DB:           f.SourceDB,
		DBConfig:     f.Source,
		MyServerId:   f.Config.MyServerId,
		ErrorHandler: f.ErrorHandler,
		Filter:       f.CopyFilter,
		TableSchema:  f.Tables,
	}
}

func (f *Ferry) NewBinlogWriter() *BinlogWriter {
	f.ensureInitialized()

	return &BinlogWriter{
		DB:               f.TargetDB,
		DatabaseRewrites: f.Config.DatabaseRewrites,
		TableRewrites:    f.Config.TableRewrites,
		Throttler:        f.Throttler,

		BatchSize:    f.Config.BinlogEventBatchSize,
		WriteRetries: f.Config.DBWriteRetries,

		ErrorHandler: f.ErrorHandler,
		StateTracker: f.StateTracker,
	}
}

func (f *Ferry) NewBinlogWriterWithoutStateTracker() *BinlogWriter {
	binlogWriter := f.NewBinlogWriter()
	binlogWriter.StateTracker = nil
	return binlogWriter
}

func (f *Ferry) NewBatchWriter() *BatchWriter {
	f.ensureInitialized()

	batchWriter := &BatchWriter{
		DB:           f.TargetDB,
		StateTracker: f.StateTracker,

		DatabaseRewrites: f.Config.DatabaseRewrites,
		TableRewrites:    f.Config.TableRewrites,

		WriteRetries: f.Config.DBWriteRetries,
	}

	batchWriter.Initialize()
	return batchWriter
}

func (f *Ferry) NewBatchWriterWithoutStateTracker() *BatchWriter {
	batchWriter := f.NewBatchWriter()
	batchWriter.StateTracker = nil
	return batchWriter
}

func (f *Ferry) NewChecksumTableVerifier() *ChecksumTableVerifier {
	f.ensureInitialized()

	return &ChecksumTableVerifier{
		SourceDB:         f.SourceDB,
		TargetDB:         f.TargetDB,
		DatabaseRewrites: f.Config.DatabaseRewrites,
		TableRewrites:    f.Config.TableRewrites,
		Tables:           f.Tables.AsSlice(),
	}
}

func (f *Ferry) NewInlineVerifier() *InlineVerifier {
	f.ensureInitialized()

	var binlogVerifyStore *BinlogVerifyStore
	if f.StateToResumeFrom != nil && f.StateToResumeFrom.BinlogVerifyStore != nil {
		binlogVerifyStore = NewBinlogVerifyStoreFromSerialized(f.StateToResumeFrom.BinlogVerifyStore)
	} else {
		binlogVerifyStore = NewBinlogVerifyStore()
	}

	return &InlineVerifier{
		SourceDB:                   f.SourceDB,
		TargetDB:                   f.TargetDB,
		DatabaseRewrites:           f.Config.DatabaseRewrites,
		TableRewrites:              f.Config.TableRewrites,
		TableSchemaCache:           f.Tables,
		BatchSize:                  f.Config.BinlogEventBatchSize,
		VerifyBinlogEventsInterval: f.Config.InlineVerifierConfig.verifyBinlogEventsInterval,
		MaxExpectedDowntime:        f.Config.InlineVerifierConfig.maxExpectedDowntime,

		StateTracker: f.StateTracker,
		ErrorHandler: f.ErrorHandler,

		reverifyStore:   binlogVerifyStore,
		sourceStmtCache: NewStmtCache(),
		targetStmtCache: NewStmtCache(),
		logger:          logrus.WithField("tag", "inline-verifier"),
	}
}

func (f *Ferry) NewInlineVerifierWithoutStateTracker() *InlineVerifier {
	v := f.NewInlineVerifier()
	v.StateTracker = nil
	return v
}

func (f *Ferry) NewIterativeVerifier() (*IterativeVerifier, error) {
	f.ensureInitialized()

	var err error
	config := f.Config.IterativeVerifierConfig

	var maxExpectedDowntime time.Duration
	if config.MaxExpectedDowntime != "" {
		maxExpectedDowntime, err = time.ParseDuration(config.MaxExpectedDowntime)
		if err != nil {
			return nil, fmt.Errorf("invalid MaxExpectedDowntime: %v. this error should have been caught via .Validate()", err)
		}
	}

	var compressionVerifier *CompressionVerifier
	if config.TableColumnCompression != nil {
		compressionVerifier, err = NewCompressionVerifier(config.TableColumnCompression)
		if err != nil {
			return nil, err
		}
	}

	ignoredColumns := make(map[string]map[string]struct{})
	for table, columns := range config.IgnoredColumns {
		ignoredColumns[table] = make(map[string]struct{})
		for _, column := range columns {
			ignoredColumns[table][column] = struct{}{}
		}
	}

	v := &IterativeVerifier{
		CursorConfig: &CursorConfig{
			DB:          f.SourceDB,
			BatchSize:   f.Config.DataIterationBatchSize,
			ReadRetries: f.Config.DBReadRetries,
		},

		BinlogStreamer:      f.BinlogStreamer,
		SourceDB:            f.SourceDB,
		TargetDB:            f.TargetDB,
		CompressionVerifier: compressionVerifier,

		Tables:              f.Tables.AsSlice(),
		TableSchemaCache:    f.Tables,
		IgnoredTables:       config.IgnoredTables,
		IgnoredColumns:      ignoredColumns,
		DatabaseRewrites:    f.Config.DatabaseRewrites,
		TableRewrites:       f.Config.TableRewrites,
		Concurrency:         config.Concurrency,
		MaxExpectedDowntime: maxExpectedDowntime,
	}

	if f.CopyFilter != nil {
		v.CursorConfig.BuildSelect = f.CopyFilter.BuildSelect
	}

	return v, v.Initialize()
}

// Initialize all the components of Ghostferry and connect to the Database
func (f *Ferry) Initialize() (err error) {
	f.StartTime = time.Now().Truncate(time.Second)
	f.OverallState = StateStarting

	f.logger = logrus.WithField("tag", "ferry")
	f.rowCopyCompleteCh = make(chan struct{})

	f.logger.Infof("hello world from %s", VersionString)

	// Suppress siddontang/go-mysql logging as we already log the equivalents.
	// It also by defaults logs to stdout, which is different from Ghostferry
	// logging, which all goes to stderr. stdout in Ghostferry is reserved for
	// dumping states due to an abort.
	siddontanglog.SetDefaultLogger(siddontanglog.NewDefault(&siddontanglog.NullHandler{}))

	// Connect to the source and target databases and check the validity
	// of the connections
	f.SourceDB, err = f.Source.SqlDB(f.logger.WithField("dbname", "source"))
	if err != nil {
		f.logger.WithError(err).Error("failed to connect to source database")
		return err
	}

	err = f.checkConnection("source", f.SourceDB)
	if err != nil {
		f.logger.WithError(err).Error("source connection checking failed")
		return err
	}

	err = f.checkConnectionForBinlogFormat(f.SourceDB)
	if err != nil {
		f.logger.WithError(err).Error("binlog format for source db is not compatible")
		return err
	}

	f.TargetDB, err = f.Target.SqlDB(f.logger.WithField("dbname", "target"))
	if err != nil {
		f.logger.WithError(err).Error("failed to connect to target database")
		return err
	}

	err = f.checkConnection("target", f.TargetDB)
	if err != nil {
		f.logger.WithError(err).Error("target connection checking failed")
		return err
	}

	isReplica, err := CheckDbIsAReplica(f.TargetDB)
	if err != nil {
		f.logger.WithError(err).Error("cannot check if target db is writable")
		return err
	}
	if isReplica {
		return fmt.Errorf("@@read_only must be OFF on target db")
	}

	// Check if we're running from a replica or not and sanity check
	// the configurations given to Ghostferry as well as the configurations
	// of the MySQL databases.
	if f.WaitUntilReplicaIsCaughtUpToMaster != nil {
		f.WaitUntilReplicaIsCaughtUpToMaster.ReplicaDB = f.SourceDB

		if f.WaitUntilReplicaIsCaughtUpToMaster.MasterDB == nil {
			err = errors.New("must specify a MasterDB")
			f.logger.WithError(err).Error("must specify a MasterDB")
			return err
		}

		err = f.checkConnection("source_master", f.WaitUntilReplicaIsCaughtUpToMaster.MasterDB)
		if err != nil {
			f.logger.WithError(err).Error("source master connection checking failed")
			return err
		}

		var zeroPosition siddontangmysql.Position
		// Ensures the query to check for position is executable.
		_, err = f.WaitUntilReplicaIsCaughtUpToMaster.IsCaughtUp(zeroPosition, 1)
		if err != nil {
			f.logger.WithError(err).Error("cannot check replicated master position on the source database")
			return err
		}

		isReplica, err := CheckDbIsAReplica(f.WaitUntilReplicaIsCaughtUpToMaster.MasterDB)
		if err != nil {
			f.logger.WithError(err).Error("cannot check if master is a read replica")
			return err
		}

		if isReplica {
			err = errors.New("source master is a read replica, not a master writer")
			f.logger.WithError(err).Error("source master is a read replica")
			return err
		}
	} else {
		isReplica, err := CheckDbIsAReplica(f.SourceDB)
		if err != nil {
			f.logger.WithError(err).Error("cannot check if source is a replica")
			return err
		}

		if isReplica {
			err = errors.New("source is a read replica. running Ghostferry with a source replica is unsafe unless WaitUntilReplicaIsCaughtUpToMaster is used")
			f.logger.WithError(err).Error("source is a read replica")
			return err
		}
	}

	// Initializing the necessary components of Ghostferry.
	if f.ErrorHandler == nil {
		f.ErrorHandler = &PanicErrorHandler{
			Ferry:             f,
			DumpStateToStdout: true,
		}
	}

	if f.Throttler == nil {
		f.Throttler = &PauserThrottler{}
	}

	if f.StateToResumeFrom == nil {
		f.StateTracker = NewStateTracker(f.DataIterationConcurrency * 10)
	} else {
		f.StateTracker = NewStateTrackerFromSerializedState(f.DataIterationConcurrency*10, f.StateToResumeFrom)
	}

	// Loads the schema of the tables that are applicable.
	// We need to do this at the beginning of the run as this is required
	// in order to determine the PaginationKey of each table as well as finding
	// which value in the binlog event correspond to which field in the
	// table.
	//
	// If this is a resuming run and the last known table schema cache is not given
	// we'll regenerate it from the source database, assuming it has not been
	// changed.
	if f.StateToResumeFrom == nil || f.StateToResumeFrom.LastKnownTableSchemaCache == nil {
		metrics.Measure("LoadTables", nil, 1.0, func() {
			f.Tables, err = LoadTables(f.SourceDB, f.TableFilter, f.CompressedColumnsForVerification, f.IgnoredColumnsForVerification, f.CascadingPaginationColumnConfig)
		})
		if err != nil {
			return err
		}
	} else {
		f.Tables = f.StateToResumeFrom.LastKnownTableSchemaCache
	}

	// The iterative verifier needs the binlog streamer so this has to be first.
	// Eventually this can be moved below the verifier initialization.
	f.BinlogStreamer = f.NewBinlogStreamer()
	f.BinlogWriter = f.NewBinlogWriter()
	f.DataIterator = f.NewDataIterator()
	f.BatchWriter = f.NewBatchWriter()

	if f.Config.VerifierType != "" {
		if f.Verifier != nil {
			return errors.New("VerifierType specified and Verifier is given. these are mutually exclusive options")
		}

		switch f.Config.VerifierType {
		case VerifierTypeIterative:
			f.Verifier, err = f.NewIterativeVerifier()
			if err != nil {
				return err
			}
		case VerifierTypeChecksumTable:
			f.Verifier = f.NewChecksumTableVerifier()
		case VerifierTypeInline:
			// TODO: eventually we should have the inlineVerifier as an "always on"
			// component. That will allow us to clean this up.
			f.inlineVerifier = f.NewInlineVerifier()
			f.Verifier = f.inlineVerifier
			f.BatchWriter.InlineVerifier = f.inlineVerifier
		case VerifierTypeNoVerification:
			// skip
		default:
			return fmt.Errorf("'%s' is not a known VerifierType", f.Config.VerifierType)
		}
	}

	f.logger.Info("ferry initialized")
	return nil
}

// Attach event listeners for Ghostferry components and connect the binlog
// streamer to the source shard
//
// Note: Binlog streaming doesn't start until Run. Here we simply connect to
// MySQL.
func (f *Ferry) Start() error {
	// Event listeners for the BinlogStreamer and DataIterator are called
	// in the order they are registered.
	// The builtin event listeners are to write the events to the target
	// database.
	// Registering the builtin event listeners in Start allows the consumer
	// of the library to register event listeners that gets called before
	// and after the data gets written to the target database.
	f.BinlogStreamer.AddEventListener(f.BinlogWriter.BufferBinlogEvents)
	f.DataIterator.AddBatchListener(f.BatchWriter.WriteRowBatch)

	if f.inlineVerifier != nil {
		f.BinlogStreamer.AddEventListener(f.inlineVerifier.binlogEventListener)
	}

	// The starting binlog coordinates must be determined first. If it is
	// determined after the DataIterator starts, the DataIterator might
	// miss some records that are inserted between the time the
	// DataIterator determines the range of IDs to copy and the time that
	// the starting binlog coordinates are determined.
	var pos BinlogPosition
	var err error
	if f.StateToResumeFrom != nil {
		pos, err = f.BinlogStreamer.ConnectBinlogStreamerToMysqlFrom(f.StateToResumeFrom.MinBinlogPosition())
	} else {
		pos, err = f.BinlogStreamer.ConnectBinlogStreamerToMysql()
	}
	if err != nil {
		return err
	}

	// If we don't set this now, there is a race condition where ghostferry
	// is terminated with some rows copied but no binlog events are written.
	// This guarantees that we are able to restart from a valid location.
	f.StateTracker.UpdateLastWrittenBinlogPosition(pos)
	if f.inlineVerifier != nil {
		f.StateTracker.UpdateLastStoredBinlogPositionForInlineVerifier(pos)
	}

	return nil
}

// Spawns the background tasks that actually perform the run.
// Wait for the background tasks to finish.
func (f *Ferry) Run() {
	f.logger.Info("starting ferry run")
	f.OverallState = StateCopying

	ctx, shutdown := context.WithCancel(context.Background())

	handleError := func(name string, err error) {
		if err != nil && err != context.Canceled {
			f.ErrorHandler.Fatal(name, err)
		}
	}

	supportingServicesWg := &sync.WaitGroup{}
	supportingServicesWg.Add(1)

	go func() {
		defer supportingServicesWg.Done()
		handleError("throttler", f.Throttler.Run(ctx))
	}()

	if f.Config.ProgressCallback.URI != "" {
		supportingServicesWg.Add(1)
		go func() {
			defer supportingServicesWg.Done()

			frequency := time.Duration(f.Config.ProgressReportFrequency) * time.Millisecond

			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(frequency):
					f.ReportProgress()
				}
			}
		}()
	}

	if f.DumpStateOnSignal {
		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

			s := <-c
			if ctx.Err() == nil {
				// Ghostferry is still running
				f.ErrorHandler.Fatal("user_interrupt", fmt.Errorf("signal received: %v", s.String()))
			} else {
				// shutdown() has been called and Ghostferry is done.
				os.Exit(0)
			}
		}()
	}

	inlineVerifierWg := &sync.WaitGroup{}
	inlineVerifierContext, stopInlineVerifier := context.WithCancel(ctx)
	if f.inlineVerifier != nil {
		inlineVerifierWg.Add(1)
		go func() {
			defer inlineVerifierWg.Done()
			f.inlineVerifier.PeriodicallyVerifyBinlogEvents(inlineVerifierContext)
		}()
	}

	binlogWg := &sync.WaitGroup{}
	binlogWg.Add(2)

	go func() {
		defer binlogWg.Done()
		f.BinlogWriter.Run()
	}()

	go func() {
		defer binlogWg.Done()

		f.BinlogStreamer.Run()
		f.BinlogWriter.Stop()
	}()

	dataIteratorWg := &sync.WaitGroup{}
	dataIteratorWg.Add(1)

	go func() {
		defer dataIteratorWg.Done()
		f.DataIterator.Run(f.Tables.AsSlice())
	}()

	dataIteratorWg.Wait()

	if f.inlineVerifier != nil {
		stopInlineVerifier()
		inlineVerifierWg.Wait()
	}

	if f.Verifier != nil {
		f.logger.Info("calling VerifyBeforeCutover")
		f.OverallState = StateVerifyBeforeCutover

		metrics.Measure("VerifyBeforeCutover", nil, 1.0, func() {
			err := f.Verifier.VerifyBeforeCutover()
			if err != nil {
				f.logger.WithError(err).Error("VerifyBeforeCutover failed")
				f.ErrorHandler.Fatal("verifier", err)
			}
		})
	}

	f.logger.Info("data copy is complete, waiting for cutover")
	f.OverallState = StateWaitingForCutover
	f.waitUntilAutomaticCutoverIsTrue()

	f.logger.Info("entering cutover phase, notifying caller that row copy is complete")
	f.OverallState = StateCutover
	f.notifyRowCopyComplete()

	// Cutover is a cooperative activity between the Ghostferry library and
	// applications built on Ghostferry:
	//
	// 1. At this point, the application should prepare to cutover, such as
	//    setting the source database to READONLY.
	// 2. Once that is complete, trigger the cutover by requesting the
	//    BinlogStreamer to stop.
	// 3. Once the binlog stops, this function will return and the cutover will
	//    be completed.

	binlogWg.Wait()

	f.logger.Info("ghostferry run is complete, shutting down auxiliary services")
	f.OverallState = StateDone
	f.DoneTime = time.Now()

	shutdown()
	supportingServicesWg.Wait()

	if f.Config.ProgressCallback.URI != "" {
		f.ReportProgress()
	}
}

func (f *Ferry) RunStandaloneDataCopy(tables []*TableSchema) error {
	if len(tables) == 0 {
		return nil
	}

	dataIterator := f.NewDataIteratorWithoutStateTracker()
	batchWriter := f.NewBatchWriterWithoutStateTracker()

	// Always use the InlineVerifier to verify the copied data here.
	dataIterator.SelectFingerprint = true
	batchWriter.InlineVerifier = f.NewInlineVerifierWithoutStateTracker()

	// BUG: if the PanicErrorHandler fires while running the standalone copy, we
	// will get an error dump even though we should not get one, which could be
	// misleading.

	dataIterator.AddBatchListener(batchWriter.WriteRowBatch)
	f.logger.WithField("tables", tables).Info("starting delta table copy in cutover")

	dataIterator.Run(tables)

	return nil
}

// Call this method and perform the cutover after this method returns.
func (f *Ferry) WaitUntilRowCopyIsComplete() {
	<-f.rowCopyCompleteCh
}

func (f *Ferry) WaitUntilBinlogStreamerCatchesUp() {
	for !f.BinlogStreamer.IsAlmostCaughtUp() {
		time.Sleep(500 * time.Millisecond)
	}
}

// After you stop writing to the source and made sure that all inflight
// transactions to the source are completed, call this method to ensure
// that the binlog streaming has caught up and stop the binlog streaming.
//
// This method will actually not shutdown the BinlogStreamer immediately.
// You will know that the BinlogStreamer finished when .Run() returns.
func (f *Ferry) FlushBinlogAndStopStreaming() {
	if f.WaitUntilReplicaIsCaughtUpToMaster != nil {
		isReplica, err := CheckDbIsAReplica(f.WaitUntilReplicaIsCaughtUpToMaster.MasterDB)
		if err != nil {
			f.ErrorHandler.Fatal("wait_replica", err)
		}

		if isReplica {
			err = errors.New("source master is no longer a master writer")
			msg := "aborting the ferry since the source master is no longer the master writer " +
				"this means that the perceived master can be lagging compared to the actual master " +
				"and lead to missed writes. aborting ferry"

			f.logger.Error(msg)
			f.ErrorHandler.Fatal("wait_replica", err)
		}

		err = f.WaitUntilReplicaIsCaughtUpToMaster.Wait()
		if err != nil {
			f.ErrorHandler.Fatal("wait_replica", err)
		}
	}

	f.BinlogStreamer.FlushAndStop()
}

func (f *Ferry) SerializeStateToJSON() (string, error) {
	if f.StateTracker == nil {
		err := errors.New("no valid StateTracker")
		return "", err
	}
	var binlogVerifyStore *BinlogVerifyStore = nil
	if f.inlineVerifier != nil {
		binlogVerifyStore = f.inlineVerifier.reverifyStore
	}

	serializedState := f.StateTracker.Serialize(f.Tables, binlogVerifyStore)

	stateBytes, err := json.MarshalIndent(serializedState, "", " ")
	return string(stateBytes), err
}

func (f *Ferry) Progress() *Progress {
	s := &Progress{
		CurrentState:  f.OverallState,
		CustomPayload: f.Config.ProgressCallback.Payload,
		VerifierType:  f.VerifierType,
	}

	s.Throttled = f.Throttler.Throttled()

	// Binlog Progress
	s.LastSuccessfulBinlogPos = f.BinlogStreamer.GetLastStreamedBinlogPosition()
	s.BinlogStreamerLag = time.Now().Sub(f.BinlogStreamer.lastProcessedEventTime).Seconds()
	s.FinalBinlogPos = f.BinlogStreamer.targetBinlogPosition

	// Table Progress
	serializedState := f.StateTracker.Serialize(nil, nil)
	s.Tables = make(map[string]TableProgress)
	targetPaginationKeys := make(map[string]uint64)
	f.DataIterator.targetPaginationKeys.Range(func(k, v interface{}) bool {
		targetPaginationKeys[k.(string)] = v.(uint64)
		return true
	})

	tables := f.Tables.AsSlice()

	for _, table := range tables {
		var currentAction string
		tableName := table.String()
		lastSuccessfulPaginationKey, foundInProgress := serializedState.LastSuccessfulPaginationKeys[tableName]

		if serializedState.CompletedTables[tableName] {
			currentAction = TableActionCompleted
		} else if foundInProgress {
			currentAction = TableActionCopying
		} else {
			currentAction = TableActionWaiting
		}

		s.Tables[tableName] = TableProgress{
			LastSuccessfulPaginationKey: lastSuccessfulPaginationKey,
			TargetPaginationKey:         targetPaginationKeys[tableName],
			CurrentAction:               currentAction,
		}
	}

	// ETA
	var totalPaginationKeysToCopy uint64 = 0
	var completedPaginationKeys uint64 = 0
	estimatedPaginationKeysPerSecond := f.StateTracker.EstimatedPaginationKeysPerSecond()
	for _, targetPaginationKey := range targetPaginationKeys {
		totalPaginationKeysToCopy += targetPaginationKey
	}

	for _, completedPaginationKey := range serializedState.LastSuccessfulPaginationKeys {
		completedPaginationKeys += completedPaginationKey
	}

	s.ETA = (time.Duration(math.Ceil(float64(totalPaginationKeysToCopy-completedPaginationKeys)/estimatedPaginationKeysPerSecond)) * time.Second).Seconds()
	s.PaginationKeysPerSecond = uint64(estimatedPaginationKeysPerSecond)
	s.TimeTaken = time.Now().Sub(f.StartTime).Seconds()

	return s
}

func (f *Ferry) ReportProgress() {
	callback := f.Config.ProgressCallback // make a copy as we need to set the Payload.
	progress := f.Progress()
	data, err := json.Marshal(progress)
	if err != nil {
		f.logger.WithError(err).Error("failed to Marshal progress struct?")
		return
	}

	callback.Payload = string(data)
	err = callback.Post(&http.Client{})
	if err != nil {
		f.logger.WithError(err).Warn("failed to post status, but that's probably okay")
	}
}

func (f *Ferry) waitUntilAutomaticCutoverIsTrue() {
	for !f.AutomaticCutover {
		time.Sleep(1 * time.Second)
		f.logger.Debug("waiting for AutomaticCutover to become true before signaling for row copy complete")
	}
}

func (f *Ferry) ensureInitialized() {
	// TODO: refactor Ferry.Initialize to a constructor.
	// Note: the constructor shouldn't have a large amount of positional argument
	//       so it is more readable.
	//
	if f.OverallState == "" {
		panic("ferry has not been initialized")
	}
}

func (f *Ferry) notifyRowCopyComplete() {
	f.rowCopyCompleteCh <- struct{}{}
}

func (f *Ferry) checkConnection(dbname string, db *sql.DB) error {
	row := db.QueryRow("SHOW STATUS LIKE 'Ssl_cipher'")
	var name, cipher string
	err := row.Scan(&name, &cipher)
	if err != nil {
		return err
	}

	hasSSL := cipher != ""

	f.logger.WithFields(logrus.Fields{
		"hasSSL":     hasSSL,
		"ssl_cipher": cipher,
		"dbname":     dbname,
	}).Infof("connected to %s", dbname)

	return nil
}

func (f *Ferry) checkConnectionForBinlogFormat(db *sql.DB) error {
	var name, value string

	row := db.QueryRow("SHOW VARIABLES LIKE 'binlog_format'")
	err := row.Scan(&name, &value)
	if err != nil {
		return err
	}

	if strings.ToUpper(value) != "ROW" {
		return fmt.Errorf("binlog_format must be ROW, not %s", value)
	}

	if !f.Config.SkipBinlogRowImageCheck {
		row = db.QueryRow("SHOW VARIABLES LIKE 'binlog_row_image'")
		err = row.Scan(&name, &value)
		if err != nil {
			return err
		}
		if strings.ToUpper(value) != "FULL" {
			return fmt.Errorf("binlog_row_image must be FULL, not %s", value)
		}
	}

	return nil
}
