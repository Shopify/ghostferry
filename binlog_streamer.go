package ghostferry

import (
	"context"
	"crypto/tls"
	sqlorig "database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const caughtUpThreshold = 10 * time.Second

// this is passed into event handlers to keep track of state of the binlog event stream.
type BinlogEventState struct {
	evPosition               mysql.Position
	isEventPositionResumable bool
	isEventPositionValid     bool
	nextFilename             string
}

type BinlogStreamer struct {
	DB           *sql.DB
	DBConfig     *DatabaseConfig
	MyServerId   uint32
	ErrorHandler ErrorHandler
	Filter       CopyFilter

	TableSchema TableSchemaCache
	LogTag      string

	binlogSyncer   *replication.BinlogSyncer
	binlogStreamer *replication.BinlogStreamer

	// These rewrite structures are used specifically for the Target
	// Verifier as it needs to map events streamed from the Target back
	// to the TableSchemaCache of the Source
	//
	// See https://github.com/Shopify/ghostferry/pull/258 for details
	DatabaseRewrites map[string]string
	TableRewrites    map[string]string

	// BinlogCoordinateMode selects whether this streamer tracks file/position
	// or GTID coordinates. An empty value is treated as file/position for
	// backwards compatibility.
	BinlogCoordinateMode BinlogCoordinateType

	// MasterFailoverRecovery, when set together with SourceReconnector, enables
	// automatic reconnection to a new source master on connection loss. Honored
	// only in GTID mode (GTID sets are server-independent). Nil disables it.
	MasterFailoverRecovery *MasterFailoverRecoveryConfig

	// SourceReconnector reconnects the source after a lost connection. It is
	// supplied by the Ferry, which owns the SourceRuntime and therefore how the
	// whole run repoints at the promoted writer. Required for failover recovery.
	SourceReconnector SourceReconnector

	lastStreamedBinlogPosition  mysql.Position
	lastResumableBinlogPosition mysql.Position
	stopAtBinlogPosition        mysql.Position

	// GTID tracking, only maintained when BinlogCoordinateMode is
	// BinlogCoordinateGTID. lastStreamedGTIDSet is the committed GTID set seen
	// so far. lastResumableGTIDSet is the committed GTID set at the last
	// transaction boundary (a safe resume point). stopAtGTIDSet is the target
	// executed set to stop at during cutover.
	//
	// These are *mysql.MysqlGTIDSet values (Go maps under the hood), mutated on
	// the streaming goroutine while Ferry.Progress() reads them (via the Get*
	// coordinate accessors) on another goroutine. Reading a set's String() or
	// Clone() iterates the map, so concurrent read+write is a data race (and a
	// fatal "concurrent map read and map write" if a stored set is ever mutated
	// in place). gtidMu serialises all access to these three fields; every
	// read and write below goes through the guarded accessors.
	gtidMu               sync.RWMutex
	lastStreamedGTIDSet  mysql.GTIDSet
	lastResumableGTIDSet mysql.GTIDSet
	stopAtGTIDSet        mysql.GTIDSet

	// inFlightGTID is the GTID of the transaction currently streamed but not yet
	// committed (set at GTIDEvent, cleared at its XIDEvent). Its rows may already
	// have been emitted downstream before lastStreamedGTIDSet advances, so
	// failover validation folds it into the applied set. Only touched by the Run
	// goroutine.
	inFlightGTID string

	// connMu guards DB and DBConfig, swapped by the Run goroutine during
	// failover recovery while FlushAndStop (another goroutine) reads them.
	connMu sync.Mutex

	lastProcessedEventTime   time.Time
	lastLagMetricEmittedTime time.Time

	stopRequested bool

	logger         Logger
	eventListeners []func([]DMLEvent) error
	// eventhandlers can be attached to binlog Replication Events
	// for any event that does not have a specific handler attached, a default eventHandler
	// is provided (defaultEventHandler). Event handlers are provided the replication binLogEvent
	// and a state object that carries information about the state of the binlog event stream.
	eventHandlers map[string]func(*replication.BinlogEvent, []byte, *BinlogEventState) ([]byte, error)
}

func (s *BinlogStreamer) ensureLogger() {
	if s.LogTag == "" {
		s.LogTag = "binlog_streamer"
	}

	if s.logger == nil {
		s.logger = LogWithField("tag", s.LogTag)
	}
}

func (s *BinlogStreamer) createBinlogSyncer() error {
	var err error

	if s.MyServerId == 0 {
		s.MyServerId, err = s.generateNewServerId()
		if err != nil {
			s.logger.WithError(err).Error("could not generate unique server_id")
			return err
		}
	}

	syncer, err := s.newBinlogSyncerFor(s.DBConfig, s.MyServerId)
	if err != nil {
		return err
	}
	s.binlogSyncer = syncer
	return nil
}

// newBinlogSyncerFor builds a BinlogSyncer targeting the given DB config and
// server id without mutating streamer state, so failover recovery can construct
// a candidate syncer before committing to it.
func (s *BinlogStreamer) newBinlogSyncerFor(dbConf *DatabaseConfig, serverID uint32) (*replication.BinlogSyncer, error) {
	var tlsConfig *tls.Config
	if dbConf.TLS != nil {
		var err error
		tlsConfig, err = dbConf.TLS.BuildConfig()
		if err != nil {
			return nil, err
		}
	}

	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:                 serverID,
		Host:                     dbConf.Host,
		Port:                     dbConf.Port,
		User:                     dbConf.User,
		Password:                 dbConf.Pass,
		TLSConfig:                tlsConfig,
		UseDecimal:               true,
		UseFloatWithTrailingZero: true,
		TimestampStringLocation:  time.UTC,
		Logger:                   NewSlogLogger(s.logger),
	}

	// When master-failover recovery is enabled, bound go-mysql's internal
	// reconnect-to-the-same-host retries. Otherwise the syncer would retry the
	// dead host forever and GetEvent would only ever surface context timeouts,
	// so our failover recovery (which reconnects to a *different* host) would
	// never be triggered. With the default (0) behavior is unchanged: infinite
	// retries against the configured host, matching pre-failover Ghostferry.
	if s.failoverRecoveryEnabled() {
		syncerConfig.MaxReconnectAttempts = s.MasterFailoverRecovery.syncerMaxReconnectAttempts()
	}

	return replication.NewBinlogSyncer(syncerConfig), nil
}

func (s *BinlogStreamer) ConnectBinlogStreamerToMysql() (mysql.Position, error) {
	s.ensureLogger()

	currentPosition, err := ShowMasterStatusBinlogPosition(s.DB)
	if err != nil {
		s.logger.WithError(err).Error("failed to read current binlog position")
		return mysql.Position{}, err
	}

	return s.ConnectBinlogStreamerToMysqlFrom(currentPosition)
}

func (s *BinlogStreamer) ConnectBinlogStreamerToMysqlFrom(startFromBinlogPosition mysql.Position) (mysql.Position, error) {
	s.ensureLogger()

	err := s.createBinlogSyncer()
	if err != nil {
		return mysql.Position{}, err
	}

	s.lastStreamedBinlogPosition = startFromBinlogPosition
	s.lastResumableBinlogPosition = startFromBinlogPosition

	s.logger.WithFields(Fields{
		"file":     s.lastStreamedBinlogPosition.Name,
		"position": s.lastStreamedBinlogPosition.Pos,
		"host":     s.DBConfig.Host,
		"port":     s.DBConfig.Port,
	}).Info("starting binlog streaming")

	s.binlogStreamer, err = s.binlogSyncer.StartSync(s.lastStreamedBinlogPosition)
	if err != nil {
		s.logger.WithError(err).Error("unable to start binlog streamer")
		return mysql.Position{}, err
	}

	return s.lastStreamedBinlogPosition, err
}

// coordinateMode returns the effective coordinate mode, treating the empty
// value as file/position for backwards compatibility.
func (s *BinlogStreamer) coordinateMode() BinlogCoordinateType {
	if s.BinlogCoordinateMode == "" {
		return BinlogCoordinateFilePosition
	}
	return s.BinlogCoordinateMode
}

// ConnectBinlogStreamerToMysqlWithCoordinate starts streaming from the current
// server coordinate for the configured BinlogCoordinateMode. It is the
// coordinate-typed counterpart of ConnectBinlogStreamerToMysql.
func (s *BinlogStreamer) ConnectBinlogStreamerToMysqlWithCoordinate() (BinlogCoordinate, error) {
	s.ensureLogger()

	switch s.coordinateMode() {
	case BinlogCoordinateGTID:
		coord, err := ReadCurrentGTIDCoordinate(s.DB)
		if err != nil {
			s.logger.WithError(err).Error("failed to read current executed GTID set")
			return BinlogCoordinate{}, err
		}
		return s.ConnectBinlogStreamerToMysqlSinceCoordinate(coord)
	default:
		pos, err := s.ConnectBinlogStreamerToMysql()
		if err != nil {
			return BinlogCoordinate{}, err
		}
		return NewFilePositionCoordinate(pos), nil
	}
}

// ConnectBinlogStreamerToMysqlSinceCoordinate starts streaming since the given
// coordinate. The coordinate type must match the streamer's configured
// BinlogCoordinateMode.
func (s *BinlogStreamer) ConnectBinlogStreamerToMysqlSinceCoordinate(startFrom BinlogCoordinate) (BinlogCoordinate, error) {
	s.ensureLogger()

	switch s.coordinateMode() {
	case BinlogCoordinateGTID:
		if !startFrom.IsGTID() {
			return BinlogCoordinate{}, fmt.Errorf("binlog streamer in GTID mode requires a GTID coordinate, got %q", startFrom.Type)
		}
		return s.connectBinlogStreamerFromGTID(startFrom)
	default:
		if !startFrom.IsFilePosition() {
			return BinlogCoordinate{}, fmt.Errorf("binlog streamer in file/position mode requires a file/position coordinate, got %q", startFrom.Type)
		}
		pos, err := s.ConnectBinlogStreamerToMysqlFrom(startFrom.Position())
		if err != nil {
			return BinlogCoordinate{}, err
		}
		return NewFilePositionCoordinate(pos), nil
	}
}

func (s *BinlogStreamer) connectBinlogStreamerFromGTID(startFrom BinlogCoordinate) (BinlogCoordinate, error) {
	err := s.createBinlogSyncer()
	if err != nil {
		return BinlogCoordinate{}, err
	}

	gtidSet, err := startFrom.ParsedGTIDSet()
	if err != nil {
		s.logger.WithError(err).Error("failed to parse starting GTID set")
		return BinlogCoordinate{}, err
	}

	// Seed both streamed and resumable GTID sets to the starting set. seedGTIDSets
	// clones under gtidMu so later mutations from event tracking never alias the
	// starting value and never race Progress()'s reads.
	s.seedGTIDSets(gtidSet)

	s.logger.WithFields(Fields{
		"gtid_set": gtidSet.String(),
		"host":     s.DBConfig.Host,
		"port":     s.DBConfig.Port,
	}).Info("starting binlog streaming from GTID set")

	s.binlogStreamer, err = s.binlogSyncer.StartSyncGTID(gtidSet)
	if err != nil {
		s.logger.WithError(err).Error("unable to start binlog streamer from GTID set")
		return BinlogCoordinate{}, err
	}

	return NewGTIDCoordinate(s.lastStreamedGTIDString()), nil
}

// failoverRecoveryEnabled reports whether automatic master-failover recovery is
// configured and usable. It requires GTID mode and a reconnector.
func (s *BinlogStreamer) failoverRecoveryEnabled() bool {
	return s.MasterFailoverRecovery != nil &&
		s.SourceReconnector != nil &&
		s.coordinateMode() == BinlogCoordinateGTID
}

// currentDB returns the active source DB handle under the connection lock. Used
// by goroutines other than Run (FlushAndStop) so they observe a consistent
// handle across a failover swap.
func (s *BinlogStreamer) currentDB() *sql.DB {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.DB
}

// currentDBConfig returns the active DBConfig under the connection lock.
func (s *BinlogStreamer) currentDBConfig() *DatabaseConfig {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.DBConfig
}

// appliedGTIDSet returns everything the streamer has emitted downstream: the
// committed streamed set plus any in-flight transaction's GTID (whose rows may
// already have reached listeners before the commit advanced the streamed set).
// Failover validation must ensure a candidate contains this whole set.
//
// It fails closed: if an in-flight GTID is present but cannot be folded in, it
// returns an error rather than an under-approximated set, so recovery refuses
// to validate a candidate against an incomplete applied set (which could let a
// diverging master through).
func (s *BinlogStreamer) appliedGTIDSet() (mysql.GTIDSet, error) {
	// Snapshot the streamed set under gtidMu; it is mutated by the XID/DDL paths
	// and read by Progress() concurrently.
	applied := s.lastStreamedGTIDClone()
	if s.inFlightGTID != "" {
		merged, err := unionGTIDStringInto(applied, s.inFlightGTID)
		if err != nil {
			return nil, fmt.Errorf("folding in-flight GTID %q into applied set: %w", s.inFlightGTID, err)
		}
		applied = merged
	}
	return applied, nil
}

// recoverFromMasterFailover reconnects the source after a lost connection and
// restarts streaming from the last resumable GTID set. It is only meaningful in
// GTID mode; the caller must guard with failoverRecoveryEnabled.
//
// Each attempt asks the SourceReconnector for a validated connection to the new
// writer (the Ferry implements this via SourceRuntime.Replace, so the whole run
// repoints in one place), then rebuilds the binlog syncer against it and
// restarts StartSyncGTID from the resumable set (replaying the interrupted
// transaction). Returns an error only when recovery is exhausted.
func (s *BinlogStreamer) recoverFromMasterFailover(cause error) error {
	cfg := s.MasterFailoverRecovery

	resumeSet := s.resumableGTIDClone()
	appliedSet, err := s.appliedGTIDSet()
	if err != nil {
		// Fail closed: without a trustworthy applied set we cannot safely
		// validate a candidate, so refuse to recover.
		return fmt.Errorf("master failover recovery: %w (original error: %v)", err, cause)
	}

	// If a cutover stop target has been recorded, the promoted master must also
	// contain it; otherwise streaming would wait forever for GTIDs the new
	// writer will never produce. Fold it into the set the candidate must
	// contain.
	if stopSet := s.stopGTIDClone(); stopSet != nil {
		merged, mergeErr := unionGTIDSets(appliedSet, stopSet)
		if mergeErr != nil {
			return fmt.Errorf("master failover recovery: folding stop target into required set: %w (original error: %v)", mergeErr, cause)
		}
		appliedSet = merged
	}

	previous := s.currentDBConfig()
	previousHost, previousPort := "", uint16(0)
	if previous != nil {
		previousHost, previousPort = previous.Host, previous.Port
	}

	s.logger.WithFields(Fields{
		"error":        cause.Error(),
		"resume_set":   gtidSetString(resumeSet),
		"applied_set":  gtidSetString(appliedSet),
		"dead_host":    previousHost,
		"dead_port":    previousPort,
		"max_attempts": cfg.MaxAttempts,
	}).Warn("source connection lost; attempting master failover recovery")

	for attempt := 1; cfg.MaxAttempts == 0 || attempt <= cfg.MaxAttempts; attempt++ {
		newDB, newConfig, err := s.SourceReconnector.Reconnect(previous, appliedSet)
		if err != nil {
			s.logger.WithError(err).WithField("attempt", attempt).Warn("failover: source reconnect failed")
			time.Sleep(cfg.retryWait())
			continue
		}

		newSyncer, newStreamer, err := s.buildSyncerFromGTID(newConfig, newDB, resumeSet)
		if err != nil {
			s.logger.WithError(err).WithField("attempt", attempt).Warn("failover: could not restart streaming on new master")
			time.Sleep(cfg.retryWait())
			continue
		}

		oldSyncer := s.binlogSyncer
		s.connMu.Lock()
		s.DB = newDB
		s.DBConfig = newConfig
		s.binlogSyncer = newSyncer
		s.binlogStreamer = newStreamer
		s.connMu.Unlock()
		// Re-seed the streamed/resumable GTID sets under gtidMu so a concurrent
		// Progress() read never observes a torn set during the swap.
		s.seedGTIDSets(cloneOrEmpty(resumeSet))
		s.inFlightGTID = ""

		if oldSyncer != nil {
			oldSyncer.Close()
		}

		s.logger.WithFields(Fields{
			"new_host":   newConfig.Host,
			"new_port":   newConfig.Port,
			"resume_set": gtidSetString(resumeSet),
			"attempt":    attempt,
		}).Info("master failover recovery succeeded; streaming resumed on new master")
		return nil
	}

	return fmt.Errorf("master failover recovery exhausted after %d attempts: %w", cfg.MaxAttempts, cause)
}

// buildSyncerFromGTID constructs (without installing) a fresh binlog syncer
// against the given DB config and starts a GTID stream from resumeSet. It has
// no side effects on installed state, so callers can discard the result on a
// later failure. The server id is generated against db (the new master).
func (s *BinlogStreamer) buildSyncerFromGTID(dbConf *DatabaseConfig, db *sql.DB, resumeSet mysql.GTIDSet) (*replication.BinlogSyncer, *replication.BinlogStreamer, error) {
	serverID, err := generateNewServerIdOn(db, s.logger)
	if err != nil {
		return nil, nil, err
	}

	syncer, err := s.newBinlogSyncerFor(dbConf, serverID)
	if err != nil {
		return nil, nil, err
	}

	streamer, err := syncer.StartSyncGTID(cloneOrEmpty(resumeSet))
	if err != nil {
		syncer.Close()
		return nil, nil, err
	}
	return syncer, streamer, nil
}

// gtidSetString renders a possibly-nil GTID set for logging.
func gtidSetString(set mysql.GTIDSet) string {
	if set == nil {
		return ""
	}
	return set.String()
}

// cloneOrEmpty clones set, returning a fresh empty MySQL GTID set when set is
// nil (a valid starting point for a fresh source).
func cloneOrEmpty(set mysql.GTIDSet) mysql.GTIDSet {
	if set == nil {
		empty, _ := mysql.ParseMysqlGTIDSet("")
		return empty
	}
	return set.Clone()
}

// the default event handler is called for replication binLogEvents that do not have a
// separate event Handler registered.

func (s *BinlogStreamer) defaultEventHandler(ev *replication.BinlogEvent, query []byte, es *BinlogEventState) ([]byte, error) {
	var err error
	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		// This event is used to keep the "current binlog filename" of the binlog streamer in sync.
		es.nextFilename = string(e.NextLogName)

		isFakeRotateEvent := ev.Header.LogPos == 0 && ev.Header.Timestamp == 0
		if isFakeRotateEvent {
			// Sometimes the RotateEvent is fake and not a real rotation. we want to ignore the log position in the header for those events
			// https://github.com/percona/percona-server/blob/3ff016a46ce2cde58d8007ec9834f958da53cbea/sql/rpl_binlog_sender.cc#L278-L287
			// https://github.com/percona/percona-server/blob/3ff016a46ce2cde58d8007ec9834f958da53cbea/sql/rpl_binlog_sender.cc#L904-L907

			// However, we can always advance our lastStreamedBinlogPosition according to its data fields
			es.evPosition = mysql.Position{
				Name: string(e.NextLogName),
				Pos:  uint32(e.Position),
			}
		}

		s.logger.WithFields(Fields{
			"new_position":  es.evPosition.Pos,
			"new_filename":  es.evPosition.Name,
			"last_position": s.lastStreamedBinlogPosition.Pos,
			"last_filename": s.lastStreamedBinlogPosition.Name,
		}).Info("binlog file rotated")
	case *replication.FormatDescriptionEvent:
		// This event is sent:
		//   1) when our replication client connects to mysql
		//   2) at the beginning of each binlog file
		//
		// For (1), if we are starting the binlog from a position that's greater
		// than BIN_LOG_HEADER_SIZE (currently, 4th byte), this event's position
		// is explicitly set to 0 and should not be considered valid according to
		// the mysql source. See:
		// https://github.com/percona/percona-server/blob/93165de1451548ff11dd32c3d3e5df0ff28cfcfa/sql/rpl_binlog_sender.cc#L1020-L1026
		es.isEventPositionValid = ev.Header.LogPos != 0
	case *replication.RowsQueryEvent:
		// A RowsQueryEvent will always precede the corresponding RowsEvent
		// if binlog_rows_query_log_events is enabled, and is used to get
		// the full query that was executed on the master (with annotations)
		// that is otherwise not possible to reconstruct
		query = ev.Event.(*replication.RowsQueryEvent).Query
	case *replication.RowsEvent:
		err = s.handleRowsEvent(ev, query)
		if err != nil {
			s.logger.WithError(err).Error("failed to handle rows event")
			s.ErrorHandler.Fatal("binlog_streamer", err)
		}
	case *replication.QueryEvent:
		// DDL and administrative statements (CREATE TABLE, GRANT, etc.) commit
		// via a QueryEvent rather than an XIDEvent, so their GTID is only
		// visible here. Without this, the streamed GTID set would never
		// advance past such a statement and a cutover whose stop target
		// includes it would hang forever. go-mysql attaches the current
		// executed GTID set (including this statement's GTID) to the event.
		//
		// Transaction-control statements ("BEGIN") also arrive as QueryEvents
		// but do NOT commit anything; their GTID is captured at the closing
		// XIDEvent instead, so we must skip them here to avoid advancing the
		// streamed set before the transaction's rows have been applied.
		if s.coordinateMode() == BinlogCoordinateGTID {
			qe := ev.Event.(*replication.QueryEvent)
			if qe.GSet != nil && !isTransactionControlQuery(qe.Query) {
				// A DDL/admin statement is its own transaction; the pre-statement
				// committed set is a safe resume point. Guarded so the swap does
				// not race Progress()'s String() read.
				s.setResumableToStreamed()
				s.setLastStreamedGTIDSet(qe.GSet)
			}
		}
	case *replication.XIDEvent, *replication.GTIDEvent:
		// With regards to DMLs, we see (at least) the following sequence
		// of events in the binlog stream:
		//
		// - GTIDEvent  <- START of transaction
		// - QueryEvent
		// - RowsQueryEvent
		// - TableMapEvent
		// - RowsEvent
		// - RowsEvent
		// - XIDEvent   <- END of transaction
		//
		// *NOTE*
		//
		// First, RowsQueryEvent is only available with `binlog_rows_query_log_events`
		// set to "ON".
		//
		// Second, there will be at least one (but potentially more) RowsEvents
		// depending on the number of rows updated in the transaction.
		//
		// Lastly, GTIDEvents will only be available if they are enabled.
		//
		// As a result, the following case will set the last resumable position for
		// interruption to EITHER the start (if using GTIDs) or the end of the
		// last transaction
		es.isEventPositionResumable = true

		// GTID tracking. go-mysql maintains the current GTID set internally and
		// attaches it to XIDEvent.GSet at commit boundaries, so we do not need
		// to reconstruct it from raw GTIDEvent SID/GNO.
		if s.coordinateMode() == BinlogCoordinateGTID {
			switch tev := ev.Event.(type) {
			case *replication.GTIDEvent:
				// Start of a transaction. A safe resume point is the committed
				// set that existed BEFORE this transaction, so that an
				// interruption replays the whole in-flight transaction.
				s.setResumableToStreamed()
				// Record this transaction's GTID as in-flight: its rows may be
				// emitted downstream before the closing XIDEvent advances
				// lastStreamedGTIDSet, so failover validation must ensure a
				// candidate master contains it. GTIDNext returns the single-GTID
				// set (uuid:gno) for this transaction. inFlightGTID is only
				// touched by this (streaming) goroutine, so it needs no lock.
				if nextSet, uerr := tev.GTIDNext(); uerr == nil {
					s.inFlightGTID = nextSet.String()
				} else {
					s.logger.WithError(uerr).Warn("could not decode in-flight GTID; failover validation may under-approximate applied set")
					s.inFlightGTID = ""
				}
			case *replication.XIDEvent:
				// End of a transaction. GSet is the committed GTID set through
				// this transaction. setLastStreamedGTIDSet clones under gtidMu to
				// avoid aliasing go-mysql's mutable internal set and to avoid
				// racing Progress()'s read.
				if tev.GSet != nil {
					s.setLastStreamedGTIDSet(tev.GSet)
				}
				// The transaction committed; nothing is in flight now.
				s.inFlightGTID = ""
			}
		}

		// Here we also reset the query event as we are either at the beginning
		// or the end of the current/next transaction. As such, the query will be
		// reset following the next RowsQueryEvent before the corresponding RowsEvent(s)
		query = nil
	}
	return query, err
}

// shouldContinueStreaming reports whether the Run loop should keep streaming.
//
// It keeps streaming until a stop has been requested AND the stop coordinate
// has been reached. The "have we reached the stop coordinate?" question is
// answered by BinlogCoordinate.HasReached, so this method is identical for
// file/position and GTID; the representation-specific mechanics live on the
// coordinate type.
func (s *BinlogStreamer) shouldContinueStreaming() bool {
	if !s.stopRequested {
		return true
	}

	// Once stopRequested is set, FlushAndStop has already recorded the stop
	// coordinate. We must NOT treat a zero/empty stop coordinate as "not yet
	// recorded": on a fresh source the executed GTID set (or binlog position)
	// can legitimately be empty, and an empty GTID set is a valid stop target
	// that any streamed set already contains. Deriving presence from IsZero()
	// here would hang cutover forever in that case.
	stop := s.GetStopBinlogCoordinate()

	reached, err := s.GetLastStreamedBinlogCoordinate().HasReached(stop)
	if err != nil {
		// A mismatch or parse error should not silently stop the stream; log
		// and keep going so a spurious error can't truncate replication.
		s.logger.WithError(err).Warn("could not evaluate stop coordinate; continuing to stream")
		return true
	}
	return !reached
}

func (s *BinlogStreamer) Run() {
	s.ensureLogger()

	defer func() {
		s.logger.WithFields(Fields{
			"stopAtBinlogPosition":       s.stopAtBinlogPosition,
			"lastStreamedBinlogPosition": s.lastStreamedBinlogPosition,
			"coordinateMode":             s.coordinateMode(),
		}).Info("exiting binlog streamer")
		s.binlogSyncer.Close()
	}()

	var query []byte
	es := BinlogEventState{}

	currentFilename := s.lastStreamedBinlogPosition.Name
	es.nextFilename = s.lastStreamedBinlogPosition.Name
	s.logger.Info("starting binlog streamer")
	for s.shouldContinueStreaming() {
		currentFilename = es.nextFilename
		var ev *replication.BinlogEvent
		var timedOut bool
		var err error

		// We wrap this code in an anonymous function so the context can be
		// properly cancelled and not cause a memory leak.
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			ev, err = s.binlogStreamer.GetEvent(ctx)
			if err == context.DeadlineExceeded {
				timedOut = true
			}
		}()

		if err != nil && err != context.DeadlineExceeded {
			// A non-timeout GetEvent error means the source connection was lost.
			// In GTID mode with failover recovery enabled, reconnect to the new
			// master and continue; otherwise this is fatal.
			if s.failoverRecoveryEnabled() {
				if recoverErr := s.recoverFromMasterFailover(err); recoverErr != nil {
					s.ErrorHandler.Fatal("binlog_streamer", recoverErr)
					return
				}
				// Recovered: reset per-iteration state so no stale RowsQueryEvent
				// annotation or filename crosses from the old stream.
				es = BinlogEventState{nextFilename: s.lastStreamedBinlogPosition.Name}
				query = nil
				continue
			}
			s.ErrorHandler.Fatal("binlog_streamer", err)
			return
		}

		if timedOut {
			s.lastProcessedEventTime = time.Now()
			continue
		}

		es.evPosition = mysql.Position{
			Name: currentFilename,
			Pos:  ev.Header.LogPos,
		}

		s.logger.WithFields(Fields{
			"position":                   es.evPosition.Pos,
			"file":                       es.evPosition.Name,
			"type":                       fmt.Sprintf("%T", ev.Event),
			"lastStreamedBinlogPosition": s.lastStreamedBinlogPosition,
		}).Debug("reached position")

		es.isEventPositionResumable = false
		es.isEventPositionValid = true

		// if there is a handler associated with this eventType, call it
		eventTypeString := ev.Header.EventType.String()
		if handler, ok := s.eventHandlers[eventTypeString]; ok {
			query, err = handler(ev, query, &es)
			if err != nil {
				s.logger.WithError(err).Error("failed to handle event")
				s.ErrorHandler.Fatal("binlog_streamer", err)
			}
		} else {
			// call the default event handler for everything else
			query, err = s.defaultEventHandler(ev, query, &es)
		}

		if es.isEventPositionValid {
			evType := fmt.Sprintf("%T", ev.Event)
			evTimestamp := ev.Header.Timestamp
			s.updateLastStreamedPosAndTime(evTimestamp, es.evPosition, evType, es.isEventPositionResumable)
		}
	}
}

// Attach an event handler to a replication BinLogEvent
// We only support attaching events to any of the events defined in
// https://github.com/go-mysql-org/go-mysql/blob/master/replication/const.go
// custom event handlers are provided the replication BinLogEvent and a state object
// that carries the current state of the binlog event stream.
func (s *BinlogStreamer) AddBinlogEventHandler(evType replication.EventType, eh func(*replication.BinlogEvent, []byte, *BinlogEventState) ([]byte, error)) error {
	// verify that event-type is valid
	// if eventTypeString is unrecognized, bail
	eventTypeString := evType.String()
	if eventTypeString == "UnknownEvent" {
		return errors.New("Unknown event type")
	}

	if s.eventHandlers == nil {
		s.eventHandlers = make(map[string]func(*replication.BinlogEvent, []byte, *BinlogEventState) ([]byte, error))
	}
	s.eventHandlers[eventTypeString] = eh
	return nil
}

func (s *BinlogStreamer) AddEventListener(listener func([]DMLEvent) error) {
	s.eventListeners = append(s.eventListeners, listener)
}

func (s *BinlogStreamer) GetLastStreamedBinlogPosition() mysql.Position {
	return s.lastStreamedBinlogPosition
}

// --- GTID set accessors -----------------------------------------------------
//
// All reads and writes of lastStreamedGTIDSet / lastResumableGTIDSet /
// stopAtGTIDSet go through these helpers so the underlying mysql.GTIDSet maps
// are never touched (String/Clone iterate them, mutations write them) without
// holding gtidMu. Writers store a Clone() so the streamer never retains a
// reference to a set another goroutine could mutate; readers return a
// gtidMu-protected String() snapshot.

// setLastStreamedGTIDSet stores a clone of set as the committed streamed set.
func (s *BinlogStreamer) setLastStreamedGTIDSet(set mysql.GTIDSet) {
	s.gtidMu.Lock()
	defer s.gtidMu.Unlock()
	if set == nil {
		s.lastStreamedGTIDSet = nil
		return
	}
	s.lastStreamedGTIDSet = set.Clone()
}

// setResumableToStreamed records the current streamed set as the resumable
// point (the pre-transaction committed set), cloning under the lock.
func (s *BinlogStreamer) setResumableToStreamed() {
	s.gtidMu.Lock()
	defer s.gtidMu.Unlock()
	if s.lastStreamedGTIDSet != nil {
		s.lastResumableGTIDSet = s.lastStreamedGTIDSet.Clone()
	}
}

// seedGTIDSets initialises both the streamed and resumable sets to set. Used
// when (re)connecting the GTID stream.
func (s *BinlogStreamer) seedGTIDSets(set mysql.GTIDSet) {
	s.gtidMu.Lock()
	defer s.gtidMu.Unlock()
	s.lastStreamedGTIDSet = set.Clone()
	s.lastResumableGTIDSet = set.Clone()
}

// setStopGTIDSet stores the cutover stop target.
func (s *BinlogStreamer) setStopGTIDSet(set mysql.GTIDSet) {
	s.gtidMu.Lock()
	defer s.gtidMu.Unlock()
	s.stopAtGTIDSet = set
}

// lastStreamedGTIDString returns the streamed set as a string under the lock,
// or "" when unset.
func (s *BinlogStreamer) lastStreamedGTIDString() string {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	if s.lastStreamedGTIDSet == nil {
		return ""
	}
	return s.lastStreamedGTIDSet.String()
}

// resumableGTIDClone returns a mutable clone of the resumable set under the
// lock, or nil when unset.
func (s *BinlogStreamer) resumableGTIDClone() mysql.GTIDSet {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	if s.lastResumableGTIDSet == nil {
		return nil
	}
	return s.lastResumableGTIDSet.Clone()
}

// lastStreamedGTIDClone returns a mutable clone of the streamed set under the
// lock, or nil when unset.
func (s *BinlogStreamer) lastStreamedGTIDClone() mysql.GTIDSet {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	if s.lastStreamedGTIDSet == nil {
		return nil
	}
	return s.lastStreamedGTIDSet.Clone()
}

// stopGTIDClone returns a mutable clone of the stop set under the lock, or nil
// when unset.
func (s *BinlogStreamer) stopGTIDClone() mysql.GTIDSet {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	if s.stopAtGTIDSet == nil {
		return nil
	}
	return s.stopAtGTIDSet.Clone()
}

// streamedGTIDCoordinate and resumableGTIDCoordinate return GTID coordinates
// snapshotted under gtidMu, for stamping onto DML events. They read the sets
// while holding the lock so the snapshot never races the streaming goroutine's
// writes. NewGTIDCoordinateFromSet clones internally, so the returned
// coordinate does not alias the streamer's set.
func (s *BinlogStreamer) streamedGTIDCoordinate() BinlogCoordinate {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	return NewGTIDCoordinateFromSet(s.lastStreamedGTIDSet)
}

func (s *BinlogStreamer) resumableGTIDCoordinate() BinlogCoordinate {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	return NewGTIDCoordinateFromSet(s.lastResumableGTIDSet)
}

// stopGTIDString returns the stop set as a string under the lock, or "" when
// unset.
func (s *BinlogStreamer) stopGTIDString() string {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	if s.stopAtGTIDSet == nil {
		return ""
	}
	return s.stopAtGTIDSet.String()
}

// GetLastStreamedBinlogCoordinate is the coordinate-typed counterpart of
// GetLastStreamedBinlogPosition. It returns a coordinate matching the
// streamer's configured BinlogCoordinateMode.
func (s *BinlogStreamer) GetLastStreamedBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID {
		return NewGTIDCoordinate(s.lastStreamedGTIDString())
	}
	return NewFilePositionCoordinate(s.lastStreamedBinlogPosition)
}

// GetStopBinlogCoordinate returns the recorded stop coordinate matching the
// streamer's configured BinlogCoordinateMode. It is zero until FlushAndStop has
// recorded a stop target.
func (s *BinlogStreamer) GetStopBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID {
		return NewGTIDCoordinate(s.stopGTIDString())
	}
	return NewFilePositionCoordinate(s.stopAtBinlogPosition)
}

// isTransactionControlQuery reports whether a QueryEvent query is a
// transaction-control statement that does not itself commit data (BEGIN).
// Such statements must not advance the committed GTID set; the enclosing
// transaction commits at its XIDEvent. Note COMMIT/ROLLBACK are normally
// represented as XIDEvents for InnoDB, but are treated defensively here too.
func isTransactionControlQuery(query []byte) bool {
	q := strings.ToUpper(strings.TrimSpace(string(query)))
	return q == "BEGIN" || q == "COMMIT" || q == "ROLLBACK"
}

func (s *BinlogStreamer) IsAlmostCaughtUp() bool {
	return time.Now().Sub(s.lastProcessedEventTime) < caughtUpThreshold
}

func (s *BinlogStreamer) FlushAndStop() {
	s.logger.Info("requesting binlog streamer to stop")
	// Must first read the stop coordinate before requesting stop.
	// Otherwise there is a race condition where stopRequested is set to true
	// but the stop coordinate is still nil/zero, which would cause the
	// BinlogStreamer to immediately exit, as it thinks that it has already
	// passed the stop coordinate.
	if s.coordinateMode() == BinlogCoordinateGTID {
		err := WithRetries(100, 600*time.Millisecond, s.logger, "read current executed GTID set", func() error {
			gtidSet, err := ReadExecutedGTIDSet(s.currentDB())
			if err != nil {
				return err
			}
			parsed, err := mysql.ParseMysqlGTIDSet(gtidSet)
			if err != nil {
				return err
			}
			// Store under gtidMu; the streaming goroutine (shouldContinueStreaming
			// via GetStopBinlogCoordinate) and Progress() read this concurrently.
			s.setStopGTIDSet(parsed)
			return nil
		})

		if err != nil {
			s.ErrorHandler.Fatal("binlog_streamer", err)
		}
		s.logger.WithField("stop_at_gtid_set", s.stopGTIDString()).Info("current stop GTID set was recorded")

		s.stopRequested = true
		return
	}

	err := WithRetries(100, 600*time.Millisecond, s.logger, "read current binlog position", func() error {
		var err error
		s.stopAtBinlogPosition, err = ShowMasterStatusBinlogPosition(s.currentDB())
		return err
	})

	if err != nil {
		s.ErrorHandler.Fatal("binlog_streamer", err)
	}
	s.logger.WithField("stop_at_position", s.stopAtBinlogPosition).Info("current stop binlog position was recorded")

	s.stopRequested = true
}

func (s *BinlogStreamer) updateLastStreamedPosAndTime(evTimestamp uint32, evPos mysql.Position, evType string, isResumablePosition bool) {
	if evPos.Pos == 0 {
		// This shouldn't happen, as the cases where it does happen are excluded and thus signal a programming error
		s.logger.Panicf("tried to advance to a zero log position: %s %d %T", evPos.Name, evPos.Pos, evType)
	}

	s.lastStreamedBinlogPosition = evPos
	if isResumablePosition {
		s.lastResumableBinlogPosition = evPos
	}

	// The first couple of events when connecting the binlog syncer (RotateEvent
	// and FormatDescriptionEvent) have a zero timestamp..  Ignore those for
	// timing updates
	if evTimestamp == 0 {
		return
	}

	eventTime := time.Unix(int64(evTimestamp), 0)
	s.lastProcessedEventTime = eventTime

	if time.Since(s.lastLagMetricEmittedTime) >= time.Second {
		lag := time.Since(eventTime)
		metrics.Gauge("BinlogStreamer.Lag", lag.Seconds(), nil, 1.0)
		s.lastLagMetricEmittedTime = time.Now()
	}
}

func (s *BinlogStreamer) handleRowsEvent(ev *replication.BinlogEvent, query []byte) error {
	rowsEvent := ev.Event.(*replication.RowsEvent)

	if ev.Header.LogPos == 0 {
		// This shouldn't happen, as rows events always have a logpos.
		s.logger.Panicf("logpos: %d %d %T", ev.Header.LogPos, ev.Header.Timestamp, ev.Event)
	}

	pos := mysql.Position{
		// The filename is only changed and visible during the RotateEvent, which
		// is handled transparently in Run().
		Name: s.lastStreamedBinlogPosition.Name,
		Pos:  ev.Header.LogPos,
	}

	db := string(rowsEvent.Table.Schema)
	if rewrittenDBName, exists := s.DatabaseRewrites[db]; exists {
		db = rewrittenDBName
	}

	table := string(rowsEvent.Table.Table)
	if rewrittenTableName, exists := s.TableRewrites[table]; exists {
		table = rewrittenTableName
	}

	tableFromSchemaCache := s.TableSchema.Get(db, table)
	if tableFromSchemaCache == nil {
		return nil
	}

	dmlEvs, err := NewBinlogDMLEvents(tableFromSchemaCache, ev, pos, s.lastResumableBinlogPosition, query)
	if err != nil {
		return err
	}

	// In GTID mode, stamp GTID coordinates onto the events so that downstream
	// consumers (binlog writer, verifiers) advance GTID-based state rather than
	// file/position. The resumable coordinate is the committed set BEFORE the
	// current transaction, so an interruption replays the whole transaction.
	if s.coordinateMode() == BinlogCoordinateGTID {
		// Snapshot both coordinates under gtidMu so the read does not race the
		// streaming goroutine's set mutations / Progress()'s reads.
		currentCoord := s.streamedGTIDCoordinate()
		resumableCoord := s.resumableGTIDCoordinate()
		for _, dmlEv := range dmlEvs {
			// SetCoordinates is an internal capability (coordinateStamper), not
			// part of the exported DMLEvent interface; all built-in events
			// satisfy it via DMLEventBase.
			if stamper, ok := dmlEv.(coordinateStamper); ok {
				stamper.SetCoordinates(currentCoord, resumableCoord)
			}
		}
	}

	events := make([]DMLEvent, 0)

	for _, dmlEv := range dmlEvs {
		if s.Filter != nil {
			applicable, err := s.Filter.ApplicableEvent(dmlEv)
			if err != nil {
				s.logger.WithError(err).Error("failed to apply filter for event")
				return err
			}
			if !applicable {
				continue
			}
		}

		events = append(events, dmlEv)

		metrics.Count("RowEvent", 1, []MetricTag{
			MetricTag{"table", dmlEv.Table()},
			MetricTag{"source", "binlog"},
		}, 1.0)
	}

	if len(events) == 0 {
		return nil
	}

	for _, listener := range s.eventListeners {
		err := listener(events)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *BinlogStreamer) generateNewServerId() (uint32, error) {
	return generateNewServerIdOn(s.DB, s.logger)
}

// generateNewServerIdOn generates a server id not already in use on the given
// server. It takes an explicit DB so failover recovery can target a server
// other than the currently-installed one.
func generateNewServerIdOn(db *sql.DB, logger Logger) (uint32, error) {
	var id uint32

	for {
		id = randomServerId()

		exists, err := idExistsOnServer(id, db)
		if err != nil {
			return 0, err
		}
		if !exists {
			break
		}

		if logger != nil {
			logger.WithField("server_id", id).Warn("server_id was taken, retrying")
		}
	}

	return id, nil
}

func idExistsOnServer(id uint32, db *sql.DB) (bool, error) {
	curIds, err := idsOnServer(db)
	if err != nil {
		return false, err
	}

	for _, idd := range curIds {
		if idd == id {
			return true, nil
		}
	}

	return false, nil
}

func idsOnServer(db *sql.DB) ([]uint32, error) {
	var query string
	var errorMsg string
	version, _ := db.QueryMySQLVersion()
	if isVersionAtLeast(version, "8.4.0") {
		query = "SHOW REPLICAS"
		errorMsg = "replicas"
	} else {
		query = "SHOW SLAVE HOSTS"
		errorMsg = "slave hosts"
	}
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("could not get %s: %s", errorMsg, err)
	}
	defer rows.Close()

	server_ids := make([]uint32, 0)
	for rows.Next() {
		var server_id uint32
		var host, port, master_id, slave_uuid sqlorig.NullString
		var columns []string

		columns, err = rows.Columns()

		// SHOW SLAVE HOSTS has a different return value for different implementations
		// i.e MySQL/Percona have 5 columns as it includes slave_uuid for MariaDB slave_uuid is omitted
		// since all other values are not used check for the amount of columns and gather only what is possible
		if err != nil {
			return nil, fmt.Errorf("could not get columns from %s: %v", errorMsg, err)
		} else if len(columns) == 5 {
			err = rows.Scan(&server_id, &host, &port, &master_id, &slave_uuid)
		} else if len(columns) == 4 {
			err = rows.Scan(&server_id, &host, &port, &master_id)
		} else {
			return nil, fmt.Errorf("could not scan %s row, err: unknown result set with %d columns: %v", query, len(columns), columns)
		}
		if err != nil {
			return nil, fmt.Errorf("could not scan %s row, err: %s", query, err.Error())
		}

		server_ids = append(server_ids, server_id)
	}

	return server_ids, nil
}
