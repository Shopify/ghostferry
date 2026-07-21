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
	var tlsConfig *tls.Config

	if s.DBConfig.TLS != nil {
		tlsConfig, err = s.DBConfig.TLS.BuildConfig()
		if err != nil {
			return err
		}
	}

	if s.MyServerId == 0 {
		s.MyServerId, err = s.generateNewServerId()
		if err != nil {
			s.logger.WithError(err).Error("could not generate unique server_id")
			return err
		}
	}

	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:                 s.MyServerId,
		Host:                     s.DBConfig.Host,
		Port:                     s.DBConfig.Port,
		User:                     s.DBConfig.User,
		Password:                 s.DBConfig.Pass,
		TLSConfig:                tlsConfig,
		UseDecimal:               true,
		UseFloatWithTrailingZero: true,
		TimestampStringLocation:  time.UTC,
		Logger:                   NewSlogLogger(s.logger),
	}

	s.binlogSyncer = replication.NewBinlogSyncer(syncerConfig)
	return nil
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
			case *replication.XIDEvent:
				// End of a transaction. GSet is the committed GTID set through
				// this transaction. setLastStreamedGTIDSet clones under gtidMu to
				// avoid aliasing go-mysql's mutable internal set and to avoid
				// racing Progress()'s read.
				if tev.GSet != nil {
					s.setLastStreamedGTIDSet(tev.GSet)
				}
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
			} else if err != nil {
				s.ErrorHandler.Fatal("binlog_streamer", err)
			}
		}()

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
			gtidSet, err := ReadExecutedGTIDSet(s.DB)
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
		s.stopAtBinlogPosition, err = ShowMasterStatusBinlogPosition(s.DB)
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
		var currentCoord BinlogCoordinate
		if s.lastStreamedGTIDSet != nil {
			currentCoord = NewGTIDCoordinate(s.lastStreamedGTIDSet.String())
		} else {
			currentCoord = NewGTIDCoordinate("")
		}
		var resumableCoord BinlogCoordinate
		if s.lastResumableGTIDSet != nil {
			resumableCoord = NewGTIDCoordinate(s.lastResumableGTIDSet.String())
		} else {
			resumableCoord = NewGTIDCoordinate("")
		}
		for _, dmlEv := range dmlEvs {
			dmlEv.SetCoordinates(currentCoord, resumableCoord)
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
	var id uint32

	for {
		id = randomServerId()

		exists, err := idExistsOnServer(id, s.DB)
		if err != nil {
			return 0, err
		}
		if !exists {
			break
		}

		s.logger.WithField("server_id", id).Warn("server_id was taken, retrying")
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
