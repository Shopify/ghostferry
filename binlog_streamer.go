package ghostferry

import (
	"context"
	"crypto/tls"
	sqlorig "database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
)

const caughtUpThreshold = 10 * time.Second

// this is passed into event handlers to keep track of state of the binlog event stream.
type BinlogEventState struct {
	evPosition               mysql.Position
	isEventPositionResumable bool
	isEventPositionValid     bool
	nextFilename             string
	inTransaction            bool
	transactionOpen          bool
	queryGTIDSet             mysql.GTIDSet
}

// Interface, not *replication.BinlogSyncer, so tests can supply a fake; go-mysql has no such seam.
type binlogSyncer interface {
	StartSync(pos mysql.Position) (*replication.BinlogStreamer, error)
	StartSyncGTID(gset mysql.GTIDSet) (*replication.BinlogStreamer, error)
	Close()
}

type BinlogStreamer struct {
	DB           *sql.DB
	DBConfig     *DatabaseConfig
	MyServerId   uint32
	ErrorHandler ErrorHandler
	Filter       CopyFilter

	TableSchema TableSchemaCache
	LogTag      string

	binlogSyncer   binlogSyncer
	binlogStreamer *replication.BinlogStreamer

	// Nil means replication.NewBinlogSyncer.
	binlogSyncerFactory func(replication.BinlogSyncerConfig) binlogSyncer

	// These rewrite structures are used specifically for the Target
	// Verifier as it needs to map events streamed from the Target back
	// to the TableSchemaCache of the Source
	//
	// See https://github.com/Shopify/ghostferry/pull/258 for details
	DatabaseRewrites map[string]string
	TableRewrites    map[string]string

	BinlogCoordinateMode BinlogCoordinateType

	// Enables reconnect to a promoted master on connection loss; GTID mode only.
	MasterFailoverRecovery *MasterFailoverRecoveryConfig

	// Required for failover recovery.
	SourceReconnector SourceReconnector

	lastStreamedBinlogPosition  mysql.Position
	lastResumableBinlogPosition mysql.Position
	stopAtBinlogPosition        mysql.Position

	// gtidMu guards the GTID sets below; they are only maintained in GTID mode.
	gtidMu               sync.RWMutex
	lastStreamedGTIDSet  mysql.GTIDSet
	lastResumableGTIDSet mysql.GTIDSet
	stopAtGTIDSet        mysql.GTIDSet

	// Open GTID group; its rows may be downstream before lastStreamedGTIDSet advances.
	// Run goroutine only.
	inFlightGTID string

	// Monotonic: every emitted GTID plus any stop target; a reseed never lowers it.
	// Run goroutine only.
	failoverRequiredGTIDSet mysql.GTIDSet

	// Guards DB, DBConfig, binlogSyncer and binlogStreamer.
	connMu sync.Mutex

	lastProcessedEventTime   time.Time
	lastLagMetricEmittedTime time.Time

	stopRequested atomic.Bool

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

// newBinlogSyncerFor builds a syncer for an arbitrary config without mutating streamer state.
func (s *BinlogStreamer) newBinlogSyncerFor(dbConf *DatabaseConfig, serverID uint32) (binlogSyncer, error) {
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

	// go-mysql's defaults (unlimited same-host retries, no heartbeat) would mask
	// a dead source and starve the failover trial; other streams keep the defaults.
	if s.failoverRecoveryEnabled() {
		syncerConfig.MaxReconnectAttempts = s.MasterFailoverRecovery.syncerMaxReconnectAttempts()
		syncerConfig.HeartbeatPeriod = failoverHeartbeatPeriod
		syncerConfig.ReadTimeout = failoverTrialReadTimeout
	}

	if s.binlogSyncerFactory != nil {
		return s.binlogSyncerFactory(syncerConfig), nil
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

// coordinateMode treats an empty BinlogCoordinateMode as file/position.
func (s *BinlogStreamer) coordinateMode() BinlogCoordinateType {
	if s.BinlogCoordinateMode == "" {
		return BinlogCoordinateFilePosition
	}
	return s.BinlogCoordinateMode
}

// currentCoordinate returns the source's current coordinate for the
// configured BinlogCoordinateMode.
func (s *BinlogStreamer) currentCoordinate() (BinlogCoordinate, error) {
	if s.coordinateMode() == BinlogCoordinateGTID {
		return ReadCurrentGTIDCoordinate(s.DB)
	}
	pos, err := ShowMasterStatusBinlogPosition(s.DB)
	if err != nil {
		return BinlogCoordinate{}, err
	}
	return NewFilePositionCoordinate(pos), nil
}

// ConnectBinlogStreamerToMysqlWithCoordinate starts streaming from the current server coordinate.
func (s *BinlogStreamer) ConnectBinlogStreamerToMysqlWithCoordinate() (BinlogCoordinate, error) {
	s.ensureLogger()

	coord, err := s.currentCoordinate()
	if err != nil {
		s.logger.WithError(err).Error("failed to read current binlog coordinate")
		return BinlogCoordinate{}, err
	}
	return s.ConnectBinlogStreamerToMysqlSinceCoordinate(coord)
}

// ConnectBinlogStreamerToMysqlSinceCoordinate starts streaming since the given coordinate, which must match the configured mode.
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
// configured and usable (GTID mode + reconnector).
func (s *BinlogStreamer) failoverRecoveryEnabled() bool {
	return s.MasterFailoverRecovery != nil &&
		s.SourceReconnector != nil &&
		s.coordinateMode() == BinlogCoordinateGTID
}

// currentDB returns the source DB handle under connMu; non-Run goroutines must
// use it to observe a failover swap consistently.
func (s *BinlogStreamer) currentDB() *sql.DB {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.DB
}

// currentDBConfig returns the active DBConfig under connMu.
func (s *BinlogStreamer) currentDBConfig() *DatabaseConfig {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.DBConfig
}

// appliedGTIDSet returns everything emitted downstream, including an open
// group's GTID (its rows may already have reached listeners). Fails closed:
// a fold error aborts rather than under-approximating.
func (s *BinlogStreamer) appliedGTIDSet() (mysql.GTIDSet, error) {
	// Snapshot under gtidMu: mutated by XID/DDL paths, read by Progress().
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

// Resets with every event, so it bounds silence, not transaction duration.
const failoverTrialReadTimeout = 10 * time.Second

// An idle promoted master must still emit something recovery can accept.
const failoverHeartbeatPeriod = time.Second

// Handler/listener failure (not transport): retrying on another master would
// re-deliver rows, so it aborts recovery.
type binlogConsumerError struct{ err error }

func (e *binlogConsumerError) Error() string { return e.err.Error() }
func (e *binlogConsumerError) Unwrap() error { return e.err }

// Monotonic: a reseed of the replay floor must never lower the requirement.
func (s *BinlogStreamer) updateFailoverRequirement() error {
	applied, err := s.appliedGTIDSet()
	if err != nil {
		return err
	}

	required, err := unionGTIDSets(s.failoverRequiredGTIDSet, applied)
	if err != nil {
		return fmt.Errorf("folding applied set into failover requirement: %w", err)
	}

	if stop := s.stopGTIDClone(); stop != nil {
		required, err = unionGTIDSets(required, stop)
		if err != nil {
			return fmt.Errorf("folding stop target into failover requirement: %w", err)
		}
	}

	s.failoverRequiredGTIDSet = required
	return nil
}

// recoverFromMasterFailover is GTID-only; guard with failoverRecoveryEnabled.
// A candidate is published only after a trial stream proves real progress
// (StartSyncGTID succeeds before the server can refuse). es/query keep the
// accepted trial's state so Run resumes at the next event.
func (s *BinlogStreamer) recoverFromMasterFailover(cause error, es *BinlogEventState, query *[]byte) error {
	cfg := s.MasterFailoverRecovery

	// Nil means uninitialised; a parsed empty set is valid on a fresh source.
	if s.resumableGTIDClone() == nil || s.lastStreamedGTIDClone() == nil {
		return fmt.Errorf("master failover recovery requires an initialized GTID state (original error: %v)", cause)
	}
	if s.MyServerId == 0 {
		return fmt.Errorf("master failover recovery requires an initialized server_id (original error: %v)", cause)
	}

	// A candidate must move past this so a replayed group cannot fake progress.
	progressBaseline := s.lastStreamedGTIDClone()

	if err := s.updateFailoverRequirement(); err != nil {
		return fmt.Errorf("master failover recovery: %w (original error: %v)", err, cause)
	}

	previous := s.currentDBConfig()
	previousHost, previousPort := "", uint16(0)
	if previous != nil {
		previousHost, previousPort = previous.Host, previous.Port
	}

	s.logger.WithFields(Fields{
		"error":        cause.Error(),
		"resume_set":   gtidSetString(s.resumableGTIDClone()),
		"required_set": gtidSetString(s.failoverRequiredGTIDSet),
		"dead_host":    previousHost,
		"dead_port":    previousPort,
		"max_attempts": cfg.MaxAttempts,
	}).Warn("source connection lost; attempting master failover recovery")

	// Two syncers sharing our server id would fight over the replication slot.
	// The published DB stays open; other consumers hold it.
	s.closeSyncer()

	lastErr := cause
	for attempt := 1; cfg.MaxAttempts == 0 || attempt <= cfg.MaxAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(cfg.retryWait())
		}

		// Capture rows or a stop target a failed trial emitted before the reseed
		// below lowers the replay floor.
		if err := s.updateFailoverRequirement(); err != nil {
			return fmt.Errorf("master failover recovery: %w (original error: %v)", err, cause)
		}
		required := s.failoverRequiredGTIDSet.Clone()
		resumeSet := s.resumableGTIDClone()

		*es = BinlogEventState{nextFilename: s.lastStreamedBinlogPosition.Name}
		*query = nil
		s.inFlightGTID = ""
		s.seedGTIDSets(resumeSet)

		var candidateSyncer binlogSyncer
		var candidateStreamer *replication.BinlogStreamer

		newDB, newConfig, err := s.SourceReconnector.Reconnect(previous, required.Clone(),
			func(db *sql.DB, candidate *DatabaseConfig) (mysql.GTIDSet, error) {
				syncer, streamer, startErr := s.buildSyncerFromGTID(candidate, resumeSet)
				if startErr != nil {
					return nil, startErr
				}

				finalRequired, trialErr := s.trialCandidateStream(streamer, es, query, progressBaseline, required)
				if trialErr != nil {
					syncer.Close()
					return nil, trialErr
				}

				candidateSyncer, candidateStreamer = syncer, streamer
				return finalRequired, nil
			})

		if newConfig != nil {
			// So the next resolution does not see the original dead host.
			previous = newConfig
		}

		if err != nil {
			// Trial passed but publication did not (e.g. revalidation of a
			// mid-trial stop target).
			if candidateSyncer != nil {
				candidateSyncer.Close()
				candidateSyncer, candidateStreamer = nil, nil
			}

			var consumerErr *binlogConsumerError
			if errors.As(err, &consumerErr) {
				return fmt.Errorf("master failover recovery aborted: %w", consumerErr.err)
			}

			lastErr = err
			s.logger.WithError(err).WithField("attempt", attempt).Warn("failover: source reconnect attempt failed")
			continue
		}

		if candidateStreamer == nil {
			return fmt.Errorf("master failover recovery: reconnector published a source without validating its stream")
		}

		// The accepted trial's event/query/GTID state is deliberately kept.
		s.connMu.Lock()
		s.DB = newDB
		s.DBConfig = newConfig
		s.binlogSyncer = candidateSyncer
		s.binlogStreamer = candidateStreamer
		s.connMu.Unlock()

		s.logger.WithFields(Fields{
			"new_host":     newConfig.Host,
			"new_port":     newConfig.Port,
			"resume_set":   gtidSetString(resumeSet),
			"streamed_set": s.lastStreamedGTIDString(),
			"attempt":      attempt,
		}).Info("master failover recovery succeeded; streaming resumed on new master")
		return nil
	}

	return fmt.Errorf("master failover recovery exhausted after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

// Returns the (possibly raised) requirement: the trial may emit rows or see a
// stop target mid-flight.
func (s *BinlogStreamer) trialCandidateStream(
	streamer *replication.BinlogStreamer,
	es *BinlogEventState,
	query *[]byte,
	progressBaseline, required mysql.GTIDSet,
) (mysql.GTIDSet, error) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), failoverTrialReadTimeout)
		ev, err := streamer.GetEvent(ctx)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("failover: candidate stream did not become usable: %w", err)
		}

		groupWasOpen := es.transactionOpen
		openGTID := s.inFlightGTID

		newQuery, err := s.processBinlogEvent(ev, es, *query)
		*query = newQuery
		if err != nil {
			return nil, &binlogConsumerError{err: err}
		}

		if !s.candidateStreamUsable(ev, groupWasOpen, openGTID, es, progressBaseline, required) {
			continue
		}

		if err := s.updateFailoverRequirement(); err != nil {
			return nil, err
		}
		return s.failoverRequiredGTIDSet.Clone(), nil
	}
}

// Only committed-group progress or a healthy idle heartbeat proves the candidate.
func (s *BinlogStreamer) candidateStreamUsable(
	ev *replication.BinlogEvent,
	groupWasOpen bool,
	openGTID string,
	es *BinlogEventState,
	progressBaseline, required mysql.GTIDSet,
) bool {
	committed := s.lastStreamedGTIDClone()
	if committed == nil {
		return false
	}

	// (a) An open group closed, its GTID committed, and the set moved past the baseline.
	if groupWasOpen && !es.transactionOpen {
		if openGTID != "" {
			groupSet, err := mysql.ParseMysqlGTIDSet(openGTID)
			if err != nil || !committed.Contain(groupSet) {
				return false
			}
		}
		return progressBaseline == nil || !progressBaseline.Contain(committed)
	}

	// (b) A heartbeat outside any group on a stream holding everything required:
	// an idle but healthy writer.
	if ev.Header.EventType == replication.HEARTBEAT_EVENT && !groupWasOpen && !es.transactionOpen {
		return required == nil || committed.Contain(required)
	}

	return false
}

// The published source DB is untouched: other consumers hold it.
func (s *BinlogStreamer) closeSyncer() {
	s.connMu.Lock()
	syncer := s.binlogSyncer
	s.binlogSyncer = nil
	s.binlogStreamer = nil
	s.connMu.Unlock()

	if syncer != nil {
		syncer.Close()
	}
}

// Reuses the run's server id: a fresh id per retry would leak replication
// slots on the master.
func (s *BinlogStreamer) buildSyncerFromGTID(dbConf *DatabaseConfig, resumeSet mysql.GTIDSet) (binlogSyncer, *replication.BinlogStreamer, error) {
	if resumeSet == nil {
		return nil, nil, fmt.Errorf("failover: cannot start a candidate stream from an uninitialized GTID set")
	}

	syncer, err := s.newBinlogSyncerFor(dbConf, s.MyServerId)
	if err != nil {
		return nil, nil, err
	}

	streamer, err := syncer.StartSyncGTID(resumeSet.Clone())
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
		// Not fatal here: a trial must be able to abandon a candidate.
		err = s.handleRowsEvent(ev, query)
		if err != nil {
			s.logger.WithError(err).Error("failed to handle rows event")
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

		// Here we also reset the query event as we are either at the beginning
		// or the end of the current/next transaction. As such, the query will be
		// reset following the next RowsQueryEvent before the corresponding RowsEvent(s)
		query = nil
	}
	return query, err
}

// shouldContinueStreaming reports whether the Run loop should keep streaming;
// any open GTID group is finished separately before stopping.
func (s *BinlogStreamer) shouldContinueStreaming() bool {
	if !s.stopRequested.Load() {
		return true
	}

	// A zero/empty stop coordinate is a valid stop target (fresh source), never "not yet recorded".
	if s.coordinateMode() == BinlogCoordinateGTID {
		s.gtidMu.RLock()
		defer s.gtidMu.RUnlock()
		if s.stopAtGTIDSet == nil {
			return false
		}
		if s.lastStreamedGTIDSet == nil {
			var empty mysql.MysqlGTIDSet
			return !empty.Contain(s.stopAtGTIDSet)
		}
		return !s.lastStreamedGTIDSet.Contain(s.stopAtGTIDSet)
	}
	stop := s.GetStopBinlogCoordinate()

	reached, err := s.GetLastStreamedBinlogCoordinate().HasReached(stop)
	if err != nil {
		// Fail open: an evaluation error must not silently truncate the stream.
		s.logger.WithError(err).Warn("could not evaluate stop coordinate; continuing to stream")
		return true
	}
	return !reached
}

// Shared with the candidate trial so candidates are judged with the run's semantics.
func (s *BinlogStreamer) processBinlogEvent(ev *replication.BinlogEvent, es *BinlogEventState, query []byte) ([]byte, error) {
	es.evPosition = mysql.Position{
		Name: es.nextFilename,
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

	var err error
	if handler, ok := s.eventHandlers[ev.Header.EventType.String()]; ok {
		query, err = handler(ev, query, es)
		if err != nil {
			s.logger.WithError(err).Error("failed to handle event")
			return query, err
		}
	} else {
		query, err = s.defaultEventHandler(ev, query, es)
		if err != nil {
			return query, err
		}
	}

	// Coordinate tracking runs even for custom handlers, only after success.
	if s.coordinateMode() == BinlogCoordinateGTID {
		if err := s.updateGTIDState(ev, es); err != nil {
			s.logger.WithError(err).Error("failed to update GTID state")
			return query, err
		}
	}

	if es.isEventPositionValid {
		s.updateLastStreamedPosAndTime(ev.Header.Timestamp, es.evPosition, fmt.Sprintf("%T", ev.Event), es.isEventPositionResumable)
	}

	return query, nil
}

func (s *BinlogStreamer) Run() {
	s.ensureLogger()

	defer func() {
		s.logger.WithFields(Fields{
			"stopAtBinlogPosition":       s.stopAtBinlogPosition,
			"lastStreamedBinlogPosition": s.lastStreamedBinlogPosition,
			"coordinateMode":             s.coordinateMode(),
		}).Info("exiting binlog streamer")
		// Recovery may have closed the failed syncer without installing one.
		if s.binlogSyncer != nil {
			s.binlogSyncer.Close()
		}
	}()

	var query []byte
	es := BinlogEventState{}

	es.nextFilename = s.lastStreamedBinlogPosition.Name
	s.logger.Info("starting binlog streamer")
	for es.transactionOpen || s.shouldContinueStreaming() {
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
			// Non-timeout GetEvent errors mean the source connection was lost.
			if s.failoverRecoveryEnabled() {
				if recoverErr := s.recoverFromMasterFailover(err, &es, &query); recoverErr != nil {
					s.ErrorHandler.Fatal("binlog_streamer", recoverErr)
					return
				}
				continue
			}
			s.ErrorHandler.Fatal("binlog_streamer", err)
			return
		}

		if timedOut {
			s.lastProcessedEventTime = time.Now()
			continue
		}

		query, err = s.processBinlogEvent(ev, &es, query)
		if err != nil {
			s.ErrorHandler.Fatal("binlog_streamer", err)
			return
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

func (s *BinlogStreamer) setLastStreamedGTIDSet(set mysql.GTIDSet) {
	s.gtidMu.Lock()
	defer s.gtidMu.Unlock()
	if set == nil {
		s.lastStreamedGTIDSet = nil
		return
	}
	s.lastStreamedGTIDSet = set.Clone()
}

// setResumableBeforeGTID excludes the incoming transaction, so checkpoints stay
// safe even when go-mysql replays a committed GTID.
func (s *BinlogStreamer) setResumableBeforeGTID(event *replication.GTIDEvent) error {
	sid, err := uuid.FromBytes(event.SID)
	if err != nil {
		return fmt.Errorf("invalid GTID SID: %w", err)
	}
	if event.GNO <= 0 || event.GNO == math.MaxInt64 {
		return fmt.Errorf("invalid GTID sequence number %d", event.GNO)
	}

	s.gtidMu.Lock()
	defer s.gtidMu.Unlock()
	if s.lastStreamedGTIDSet == nil {
		s.lastResumableGTIDSet = &mysql.MysqlGTIDSet{Sets: make(map[string]*mysql.UUIDSet)}
		return nil
	}
	committed, ok := s.lastStreamedGTIDSet.(*mysql.MysqlGTIDSet)
	if !ok || committed == nil {
		return fmt.Errorf("GTID resume floor requires a MySQL GTID set, got %T", s.lastStreamedGTIDSet)
	}
	floor := committed.Clone().(*mysql.MysqlGTIDSet)
	singleton := mysql.UUIDSet{SID: sid, Intervals: mysql.IntervalSlice{{Start: event.GNO, Stop: event.GNO + 1}}}
	floor.MinusSet(&singleton)
	s.lastResumableGTIDSet = floor
	return nil
}

func (s *BinlogStreamer) seedGTIDSets(set mysql.GTIDSet) {
	s.gtidMu.Lock()
	defer s.gtidMu.Unlock()
	s.lastStreamedGTIDSet = set.Clone()
	s.lastResumableGTIDSet = set.Clone()
}

func (s *BinlogStreamer) setStopGTIDSet(set mysql.GTIDSet) {
	s.gtidMu.Lock()
	defer s.gtidMu.Unlock()
	s.stopAtGTIDSet = set
}

func (s *BinlogStreamer) lastStreamedGTIDString() string {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	if s.lastStreamedGTIDSet == nil {
		return ""
	}
	return s.lastStreamedGTIDSet.String()
}

func (s *BinlogStreamer) resumableGTIDClone() mysql.GTIDSet {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	if s.lastResumableGTIDSet == nil {
		return nil
	}
	return s.lastResumableGTIDSet.Clone()
}

// lastStreamedGTIDClone returns a clone under gtidMu, nil when unset.
func (s *BinlogStreamer) lastStreamedGTIDClone() mysql.GTIDSet {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	if s.lastStreamedGTIDSet == nil {
		return nil
	}
	return s.lastStreamedGTIDSet.Clone()
}

// stopGTIDClone returns a clone under gtidMu, nil when unset.
func (s *BinlogStreamer) stopGTIDClone() mysql.GTIDSet {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	if s.stopAtGTIDSet == nil {
		return nil
	}
	return s.stopAtGTIDSet.Clone()
}

// NewGTIDCoordinateFromSet clones, so the returned coordinates do not alias the
// streamer's sets.
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

func (s *BinlogStreamer) stopGTIDString() string {
	s.gtidMu.RLock()
	defer s.gtidMu.RUnlock()
	if s.stopAtGTIDSet == nil {
		return ""
	}
	return s.stopAtGTIDSet.String()
}

func (s *BinlogStreamer) GetLastStreamedBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID {
		return NewGTIDCoordinate(s.lastStreamedGTIDString())
	}
	return NewFilePositionCoordinate(s.lastStreamedBinlogPosition)
}

// GetStopBinlogCoordinate returns the stop coordinate recorded by FlushAndStop.
func (s *BinlogStreamer) GetStopBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID {
		return NewGTIDCoordinate(s.stopGTIDString())
	}
	return NewFilePositionCoordinate(s.stopAtBinlogPosition)
}

// updateGTIDState tracks transaction boundaries independently of event handlers.
// go-mysql's GSet includes the in-flight GTID even on BEGIN and SAVEPOINT, so
// QueryEvents inside an explicit transaction must not advance the committed set.
func (s *BinlogStreamer) updateGTIDState(ev *replication.BinlogEvent, es *BinlogEventState) error {
	// XA_PREPARE_LOG_EVENT (decoded as GenericEvent) terminates the GTID group;
	// XA END does not, so its snapshot is retained until this marker.
	if ev.Header.EventType == replication.XA_PREPARE_LOG_EVENT {
		s.commitGTIDState(es.queryGTIDSet, es)
		return nil
	}
	switch e := ev.Event.(type) {
	case *replication.GTIDEvent:
		es.transactionOpen = true
		es.inTransaction = false
		es.queryGTIDSet = nil
		if err := s.setResumableBeforeGTID(e); err != nil {
			return err
		}
		// Fail closed: an under-approximated applied set could let a diverging
		// master pass validation.
		nextSet, err := e.GTIDNext()
		if err != nil {
			return fmt.Errorf("decoding in-flight GTID: %w", err)
		}
		s.inFlightGTID = nextSet.String()
	case *replication.QueryEvent:
		es.queryGTIDSet = e.GSet
		query := strings.TrimSpace(string(e.Query))
		if strings.EqualFold(query, "BEGIN") || isXAStart(query) {
			es.inTransaction = true
			return nil
		}
		if strings.EqualFold(query, "COMMIT") || strings.EqualFold(query, "ROLLBACK") {
			es.inTransaction = false
		}
		if !es.inTransaction && e.GSet != nil {
			s.commitGTIDState(e.GSet, es)
		}
	case *replication.XIDEvent:
		s.commitGTIDState(e.GSet, es)
	}
	return nil
}

func isXAStart(query string) bool {
	if len(query) < 2 || !strings.EqualFold(query[:2], "XA") {
		return false
	}
	first := true
	for token := range strings.FieldsSeq(query) {
		if first {
			if !strings.EqualFold(token, "XA") {
				return false
			}
			first = false
			continue
		}
		return strings.EqualFold(token, "START") || strings.EqualFold(token, "BEGIN")
	}
	return false
}

func (s *BinlogStreamer) commitGTIDState(set mysql.GTIDSet, es *BinlogEventState) {
	if set != nil {
		s.setLastStreamedGTIDSet(set)
	}
	es.inTransaction = false
	es.transactionOpen = false
	es.queryGTIDSet = nil
	s.inFlightGTID = ""
}

func (s *BinlogStreamer) IsAlmostCaughtUp() bool {
	return time.Now().Sub(s.lastProcessedEventTime) < caughtUpThreshold
}

func (s *BinlogStreamer) FlushAndStop() {
	s.logger.Info("requesting binlog streamer to stop")
	// Must first read the stop coordinate before requesting stop, or the
	// streamer exits immediately thinking it already passed the stop coordinate.
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
			s.setStopGTIDSet(parsed)
			return nil
		})

		if err != nil {
			s.ErrorHandler.Fatal("binlog_streamer", err)
		}
		s.logger.WithField("stop_at_gtid_set", s.stopGTIDString()).Info("current stop GTID set was recorded")

		s.stopRequested.Store(true)
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

	s.stopRequested.Store(true)
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

	// The resumable coordinate excludes the current transaction so an
	// interruption replays it whole.
	if s.coordinateMode() == BinlogCoordinateGTID {
		currentCoord := s.streamedGTIDCoordinate()
		resumableCoord := s.resumableGTIDCoordinate()
		for _, dmlEv := range dmlEvs {
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

// generateNewServerIdOn generates a server id unused on db; the explicit handle
// lets failover target a candidate master.
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
