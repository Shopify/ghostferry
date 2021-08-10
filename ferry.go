package ghostferry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-sql-driver/mysql"
	siddontanglog "github.com/siddontang/go-log/log"
	siddontangmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

var (
	zeroPosition  siddontangmysql.Position
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

	targetVerifierWg *sync.WaitGroup
	TargetVerifier   *TargetVerifier

	DataIterator *DataIterator
	DataIterators []*DataIterator
	ShardingValues []int64
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
	OverallState atomic.Value

	logger *logrus.Entry

	rowCopyCompleteCh chan struct{}

	ManagementEndpointState ManagementEndpointState
}

type ManagementEndpointState struct {
	changeListeners []func(ManagementRequestPayload) error
}

func (m *ManagementEndpointState) AddChangeListener(listener func(ManagementRequestPayload) error) {
	m.changeListeners = append(m.changeListeners, listener)
}

func (m *ManagementEndpointState) Notify(payload ManagementRequestPayload) {
	for _, l := range m.changeListeners {
		l(payload)
	}
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

			BatchSize:                 f.Config.DataIterationBatchSize,
			BatchSizePerTableOverride: f.Config.DataIterationBatchSizePerTableOverride,
			ReadRetries:               f.Config.DBReadRetries,
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

func (f *Ferry) NewSourceBinlogStreamer() *BinlogStreamer {
	return f.newBinlogStreamer(f.SourceDB, f.Config.Source, nil, nil, "source_binlog_streamer")
}

func (f *Ferry) NewTargetBinlogStreamer() (*BinlogStreamer, error) {
	schemaRewrites, err := TargetToSourceRewrites(f.Config.DatabaseRewrites)
	if err != nil {
		return nil, err
	}

	tableRewrites, err := TargetToSourceRewrites(f.Config.TableRewrites)
	if err != nil {
		return nil, err
	}

	return f.newBinlogStreamer(f.TargetDB, f.Config.Target, schemaRewrites, tableRewrites, "target_binlog_streamer"), nil
}

func (f *Ferry) newBinlogStreamer(db *sql.DB, dbConf *DatabaseConfig, schemaRewrites, tableRewrites map[string]string, logTag string) *BinlogStreamer {
	f.ensureInitialized()

	// if f.StreamFilter == nil {
	// 	panic("f.StreamFilter cant be nil")
	// }

	return &BinlogStreamer{
		DB:               db,
		DBConfig:         dbConf,
		MyServerId:       f.Config.MyServerId,
		ErrorHandler:     f.ErrorHandler,
		Filter:           f.StreamFilter,
		TableSchema:      f.Tables,
		LogTag:           logTag,
		DatabaseRewrites: schemaRewrites,
		TableRewrites:    tableRewrites,
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

		WriteRetries:       f.Config.DBWriteRetries,
		enableRowBatchSize: f.Config.EnableRowBatchSize,
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
			DB:                        f.SourceDB,
			BatchSize:                 f.Config.DataIterationBatchSize,
			BatchSizePerTableOverride: f.Config.DataIterationBatchSizePerTableOverride,
			ReadRetries:               f.Config.DBReadRetries,
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
	f.OverallState.Store(StateStarting)

	f.logger = logrus.WithField("tag", "ferry")
	f.rowCopyCompleteCh = make(chan struct{})

	f.logger.Infof("hello world from %s", VersionString)

	// Kind of a hack for now. The ErrorHandler is originally intended to be
	// passed in by the application. This here should only set a default value in
	// case one is not passed in. However, ghostferry-sharding currently depend
	// on ferry.ErrorHandler being not-nil, as the sanity checks performed at the
	// beginning of this method will get its errors reported by the ErrorHandler.
	// Since f88c58523988b9fc98f14c6a69c138279f257fe6 we no longer initialize the
	// ErrorHandler outside of Ferry, meaning that we need to initialize this as
	// early as possible.
	if f.ErrorHandler == nil {
		f.ErrorHandler = &PanicErrorHandler{
			Ferry:                    f,
			ErrorCallback:            f.Config.ErrorCallback,
			DumpStateToStdoutOnError: f.Config.DumpStateToStdoutOnError,
		}
	}

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

	if f.Throttler == nil {
		f.Throttler = &PauserThrottler{}
	}

	if f.StateToResumeFrom == nil {
		f.StateTracker = NewStateTracker(f.DataIterationConcurrency * 10)
	} else {
		f.logger.WithFields(logrus.Fields{
			"LastWrittenBinlogPosition":                 f.StateToResumeFrom.LastWrittenBinlogPosition,
			"LastStoredBinlogPositionForInlineVerifier": f.StateToResumeFrom.LastStoredBinlogPositionForInlineVerifier,
			"LastStoredBinlogPositionForTargetVerifier": f.StateToResumeFrom.LastStoredBinlogPositionForTargetVerifier,
		}).Info("resuming from state")
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
			f.Tables, err = LoadTables(f.SourceDB, f.TableFilter, f.CompressedColumnsForVerification, f.IgnoredColumnsForVerification, f.ForceIndexForVerification, f.CascadingPaginationColumnConfig)
		})
		if err != nil {
			return err
		}
	} else {
		f.Tables = f.StateToResumeFrom.LastKnownTableSchemaCache
	}

	if !f.Config.SkipForeignKeyConstraintsCheck {
		err = f.checkSourceForeignKeyConstraints()

		if err != nil {
			f.logger.WithError(err).Error("foreign key constraints detected on the source database. Enable SkipForeignKeyConstraintsCheck to ignore this check and allow foreign keys")
			return err
		}
	}

	if f.Config.DataIterationBatchSizePerTableOverride != nil {
		err = f.Config.DataIterationBatchSizePerTableOverride.UpdateBatchSizes(f.SourceDB, f.Tables)
		if err != nil {
			f.logger.WithError(err).Warn("Failed to update batch sizes for all tables")
		}
	}

	// The iterative verifier needs the binlog streamer so this has to be first.
	// Eventually this can be moved below the verifier initialization.
	f.BinlogStreamer = f.NewSourceBinlogStreamer()

	if !f.Config.SkipTargetVerification {
		targetBinlogStreamer, err := f.NewTargetBinlogStreamer()
		if err != nil {
			return err
		}

		targetVerifier, err := NewTargetVerifier(f.TargetDB, f.StateTracker, targetBinlogStreamer)
		if err != nil {
			return err
		}

		f.TargetVerifier = targetVerifier
	}

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
	var sourcePos siddontangmysql.Position
	var targetPos siddontangmysql.Position

	var err error
	if f.StateToResumeFrom != nil {
		sourcePos, err = f.BinlogStreamer.ConnectBinlogStreamerToMysqlFrom(f.StateToResumeFrom.MinSourceBinlogPosition())
	} else {
		sourcePos, err = f.BinlogStreamer.ConnectBinlogStreamerToMysql()
	}
	if err != nil {
		return err
	}

	if !f.Config.SkipTargetVerification {
		if f.StateToResumeFrom != nil && f.StateToResumeFrom.LastStoredBinlogPositionForTargetVerifier != zeroPosition {
			targetPos, err = f.TargetVerifier.BinlogStreamer.ConnectBinlogStreamerToMysqlFrom(f.StateToResumeFrom.LastStoredBinlogPositionForTargetVerifier)
		} else {
			targetPos, err = f.TargetVerifier.BinlogStreamer.ConnectBinlogStreamerToMysql()
		}
	}
	if err != nil {
		return err
	}

	// If we don't set this now, there is a race condition where Ghostferry
	// is terminated with some rows copied but no binlog events are written.
	// This guarentees that we are able to restart from a valid location.
	f.StateTracker.UpdateLastResumableSourceBinlogPosition(sourcePos)
	if f.inlineVerifier != nil {
		f.StateTracker.UpdateLastResumableSourceBinlogPositionForInlineVerifier(sourcePos)
	}

	if !f.Config.SkipTargetVerification {
		f.StateTracker.UpdateLastResumableBinlogPositionForTargetVerifier(targetPos)
	}

	return nil
}

type ShardingValueOperation struct {
	Operation string
	Value     interface{}
}

type ManagementRequestPayload struct {
	ShardingValue ShardingValueOperation
}

// Spawns the background tasks that actually perform the run.
// Wait for the background tasks to finish.
func (f *Ferry) Run() {
	f.logger.Info("starting ferry run")
	f.OverallState.Store(StateCopying)

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

	if f.Config.ManagementEndpoint != "" {
		supportingServicesWg.Add(1)
		// curl -v http://localhost:8081/ --data '{"ShardingValue": {"Operation": "add", "Value": 1}}'
		go func() {
			defer supportingServicesWg.Done()

			s := &http.Server{
				Addr: f.Config.ManagementEndpoint,
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var input ManagementRequestPayload
					err := json.NewDecoder(r.Body).Decode(&input)
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					f.ManagementEndpointState.Notify(input)
					log.Printf("ManagementEndpoint hit, payload: %v\n", input)
				}),
				ReadTimeout:  10 * time.Second,
				WriteTimeout: 10 * time.Second,
			}
			f.logger.WithField("address", f.Config.ManagementEndpoint).Info("starting management server")
			log.Fatal(s.ListenAndServe())
		}()
	}

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

	if f.Config.StateCallback.URI != "" {
		supportingServicesWg.Add(1)
		go func() {
			defer supportingServicesWg.Done()

			frequency := time.Duration(f.Config.StateReportFrequency) * time.Millisecond

			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(frequency):
					f.ReportState()
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
				if f.OverallState.Load() != StateCutover {
					// Save state dump and exit if not during the cutover stage
					f.ErrorHandler.Fatal("user_interrupt", fmt.Errorf("signal received: %v", s.String()))
				} else {
					// Log and ignore the signal during cutover
					f.logger.Warnf("Received signal: %s during cutover. "+
						"Refusing to interrupt and will attempt to complete the run.", s.String())
				}
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
	binlogWg.Add(1)
	go func() {
		defer binlogWg.Done()
		f.BinlogWriter.Run()
	}()

	binlogWg.Add(1)
	go func() {
		defer binlogWg.Done()

		f.BinlogStreamer.Run()
		f.BinlogWriter.Stop()
	}()

	if !f.Config.SkipTargetVerification {
		f.targetVerifierWg = &sync.WaitGroup{}
		f.targetVerifierWg.Add(1)
		go func() {
			defer f.targetVerifierWg.Done()
			f.TargetVerifier.BinlogStreamer.Run()
		}()
	}

	dataIteratorWg := &sync.WaitGroup{}
	dataIteratorWg.Add(1)
	go func() {
		defer dataIteratorWg.Done()
		f.DataIterator.Run(f.Tables.AsSlice())
	}()

	dataIteratorWg.Wait()

	dataIteratorsWg := &sync.WaitGroup{}
	for _, iterator := range f.DataIterators {
		dataIteratorsWg.Add(1)

		go func(iterator *DataIterator) {
			defer dataIteratorsWg.Done()
			iterator.Run(f.Tables.AsSlice())
		}(iterator)
	}

	dataIteratorsWg.Wait()

	f.logger.Info("data copy is complete, waiting for cutover")
	f.OverallState.Store(StateWaitingForCutover)
	f.waitUntilAutomaticCutoverIsTrue()

	if f.inlineVerifier != nil {
		// Stops the periodic verification of binlogs in the inline verifier
		// This should be okay as we enqueue the binlog events into the verifier,
		// which will be verified both in VerifyBeforeCutover and
		// VerifyDuringCutover.
		stopInlineVerifier()
		inlineVerifierWg.Wait()
	}

	if f.Verifier != nil {
		f.logger.Info("calling VerifyBeforeCutover")
		f.OverallState.Store(StateVerifyBeforeCutover)

		metrics.Measure("VerifyBeforeCutover", nil, 1.0, func() {
			err := f.Verifier.VerifyBeforeCutover()
			if err != nil {
				f.logger.WithError(err).Error("VerifyBeforeCutover failed")
				f.ErrorHandler.Fatal("verifier", err)
			}
		})
	}

	// Cutover is a cooperative activity between the Ghostferry library and
	// applications built on Ghostferry:
	//
	// 1. At this point (before stopping the binlog streaming), the application
	//    should prepare to cutover, such as setting the source database to
	//    READONLY.
	// 2. Once that is complete, trigger the cutover by requesting the
	//    BinlogStreamer to stop (WaitUntilBinlogStreamerCatchesUp and
	//    FlushBinlogAndStopStreaming).
	// 3. Once the binlog stops, this Run function will return and the cutover
	//    will be completed. Application and human operators can perform
	//    additional operations, such as additional verification, and repointing
	//    any consumers of the source database to use the target database.
	//
	// During cutover, if verifiers are enabled, VerifyDuringCutover should be
	// called. This can be performed as a part of the ControlServer, if that
	// component is used.

	f.logger.Info("entering cutover phase, notifying caller that row copy is complete")
	f.OverallState.Store(StateCutover)
	if f.Config.ProgressCallback.URI != "" {
		f.ReportProgress()
	}
	f.notifyRowCopyComplete()

	binlogWg.Wait()

	f.logger.Info("ghostferry run is complete, shutting down auxiliary services")
	f.OverallState.Store(StateDone)
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
	batchWriter.EnforceInlineVerification = true // Don't have the Binlog component at this stage, so no reverify

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
	for !f.BinlogStreamer.IsAlmostCaughtUp() ||
		(!f.Config.SkipTargetVerification && !f.TargetVerifier.BinlogStreamer.IsAlmostCaughtUp()) {
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

func (f *Ferry) StopTargetVerifier() {
	if !f.Config.SkipTargetVerification {
		f.TargetVerifier.BinlogStreamer.FlushAndStop()
		f.targetVerifierWg.Wait()
	}
}

func (f *Ferry) StartCutover() time.Time {
	var (
		err          error
		cutoverStart time.Time
	)
	err = WithRetries(f.Config.MaxCutoverRetries, time.Duration(f.Config.CutoverRetryWaitSeconds)*time.Second, f.logger, "get cutover lock", func() (err error) {
		metrics.Measure("CutoverLock", nil, 1.0, func() {
			cutoverStart = time.Now()
			err = f.Config.CutoverLock.Post(&http.Client{})
		})
		return err
	})

	if err != nil {
		f.logger.WithField("error", err).Errorf("locking failed, aborting run")
		f.ErrorHandler.Fatal("cutover", err)
	}
	return cutoverStart
}

func (f *Ferry) EndCutover(cutoverStart time.Time) {
	var err error
	metrics.Measure("CutoverUnlock", nil, 1.0, func() {
		err = f.Config.CutoverUnlock.Post(&http.Client{})
	})
	if err != nil {
		f.logger.WithField("error", err).Errorf("unlocking failed, aborting run")
		f.ErrorHandler.Fatal("cutover", err)
	}

	metrics.Timer("CutoverTime", time.Since(cutoverStart), nil, 1.0)
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

	if f.Config.DoNotIncludeSchemaCacheInStateDump {
		serializedState.LastKnownTableSchemaCache = nil
	}

	stateBytes, err := json.MarshalIndent(serializedState, "", " ")
	return string(stateBytes), err
}

func (f *Ferry) Progress() *Progress {
	s := &Progress{
		CurrentState:  f.OverallState.Load().(string),
		CustomPayload: f.Config.ProgressCallback.Payload,
		VerifierType:  f.VerifierType,
	}

	s.Throttled = f.Throttler.Throttled()

	now := time.Now()

	// Binlog Progress
	s.LastSuccessfulBinlogPos = f.BinlogStreamer.lastStreamedBinlogPosition
	s.BinlogStreamerLag = now.Sub(f.BinlogStreamer.lastProcessedEventTime).Seconds()
	s.BinlogWriterLag = now.Sub(f.BinlogWriter.lastProcessedEventTime).Seconds()
	s.FinalBinlogPos = f.BinlogStreamer.stopAtBinlogPosition

	if f.TargetVerifier != nil {
		s.TargetBinlogStreamerLag = now.Sub(f.TargetVerifier.BinlogStreamer.lastProcessedEventTime).Seconds()
	}

	// Table Progress
	serializedState := f.StateTracker.Serialize(nil, nil)
	// Note the below will not necessarily be synchronized with serializedState.
	// This is fine as we don't need to be super precise with performance data.
	rowStatsWrittenPerTable := f.StateTracker.RowStatsWrittenPerTable()

	s.Tables = make(map[string]TableProgress)
	targetPaginationKeys := make(map[string]uint64)
	f.DataIterator.targetPaginationKeys.Range(func(k, v interface{}) bool {
		targetPaginationKeys[k.(string)] = v.(uint64)
		return true
	})

	// Verifier information
	if f.Verifier != nil {
		s.VerifierMessage = f.Verifier.Message()
	}

	tables := f.Tables.AsSlice()

	for _, table := range tables {
		var currentAction string
		tableName := table.String()
		lastSuccessfulPaginationKey, foundInProgress := serializedState.LastSuccessfulPaginationKeys[tableName]

		if serializedState.CompletedTables[tableName] {
			currentAction = TableActionCompleted
		} else if foundInProgress {
			currentAction = TableActionCopying
			s.ActiveDataIterators += 1
		} else {
			currentAction = TableActionWaiting
		}

		rowWrittenStats, _ := rowStatsWrittenPerTable[tableName]

		s.Tables[tableName] = TableProgress{
			LastSuccessfulPaginationKey: lastSuccessfulPaginationKey,
			TargetPaginationKey:         targetPaginationKeys[tableName],
			CurrentAction:               currentAction,
			BatchSize:                   f.DataIterator.CursorConfig.GetBatchSize(table.Schema, table.Name),
			RowsWritten:                 rowWrittenStats.NumRows,
			BytesWritten:                rowWrittenStats.NumBytes,
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
	var remainingPaginationKeys float64 = 0

	// We do this because if rows are inserted between when ghostferry is started and when all the copy is completed
	// we can end up with a negative value.
	remainingPaginationKeys = math.Max(0, float64(totalPaginationKeysToCopy-completedPaginationKeys))
	s.ETA = (time.Duration(math.Ceil(remainingPaginationKeys/estimatedPaginationKeysPerSecond)) * time.Second).Seconds()
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

// ReportState may have a slight performance impact as it will temporarily
// lock the StateTracker when it is serialized before posting to the callback
func (f *Ferry) ReportState() {
	callback := f.Config.StateCallback
	state, err := f.SerializeStateToJSON()
	if err != nil {
		f.logger.Panicf("failed to serialize state to JSON: %s", err)
	}

	callback.Payload = string(state)
	err = callback.Post(&http.Client{})
	if err != nil {
		f.logger.Panicf("failed to post state to callback: %s with err: %s", callback, err)
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
	if f.OverallState.Load() == nil || f.OverallState.Load() == "" {
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

	if !f.Config.SkipTargetVerification {
		row = db.QueryRow("SHOW VARIABLES LIKE 'binlog_rows_query_log_events'")
		err = row.Scan(&name, &value)
		if err != nil {
			return err
		}
		if strings.ToUpper(value) != "ON" {
			return fmt.Errorf("binlog_rows_query_log_events must be ON, not %s", value)
		}
	}

	return nil
}

func (f *Ferry) checkSourceForeignKeyConstraints() error {
	for _, table := range f.Tables {
		query := fmt.Sprintf(`
		SELECT COUNT(*), constraint_name
		FROM information_schema.table_constraints
		WHERE constraint_type = 'FOREIGN KEY'
		AND table_schema='%s'
		AND table_name='%s'
		GROUP BY constraint_name
		`,
			table.Schema,
			table.Name,
		)

		rows, err := f.SourceDB.Query(query)
		defer rows.Close()

		var count int
		var name string

		for rows.Next() {
			err = rows.Scan(&count, &name)

			if err != nil {
				return err
			}

			if count > 0 {
				return fmt.Errorf("found at least 1 foreign key constraint on source DB. table: %s.%s, constraint: %s", table.Schema, table.Name, name)
			}
		}
	}

	return nil
}
