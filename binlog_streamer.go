package ghostferry

import (
	sqlorig "database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const caughtUpThreshold = 10 * time.Second

// DDLEventHandler is an optional hook invoked for every schema-changing DDL
// statement observed on the stream (canal's OnDDL). Returning an error aborts
// the streamer. It replaces the previous raw AddBinlogEventHandler mechanism,
// whose only real use was intercepting DDL QueryEvents.
type DDLEventHandler func(schemaName, tableName string, query []byte) error

type BinlogStreamer struct {
	DB           *sql.DB
	DBConfig     *DatabaseConfig
	MyServerId   uint32
	ErrorHandler ErrorHandler
	Filter       CopyFilter

	TableSchema TableSchemaCache
	LogTag      string

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

	// DDLEventHandler, if set, is invoked for each schema-changing DDL.
	DDLEventHandler DDLEventHandler

	canal *canal.Canal

	lastStreamedBinlogPosition  mysql.Position
	lastResumableBinlogPosition mysql.Position
	stopAtBinlogPosition        mysql.Position

	// GTID tracking, only maintained when BinlogCoordinateMode is
	// BinlogCoordinateGTID. lastStreamedGTIDSet is the committed GTID set seen
	// so far. lastResumableGTIDSet is the committed GTID set at the last
	// transaction boundary (a safe resume point). stopAtGTIDSet is the target
	// executed set to stop at during cutover.
	lastStreamedGTIDSet  mysql.GTIDSet
	lastResumableGTIDSet mysql.GTIDSet
	stopAtGTIDSet        mysql.GTIDSet

	lastProcessedEventTime   time.Time
	lastLagMetricEmittedTime time.Time

	stopRequested bool

	logger         Logger
	eventListeners []func([]DMLEvent) error

	// stopOnce guards canal.Close so the stop monitor and Run's defer never
	// double-close the canal (canal.Close is not safe to call concurrently
	// with itself while run() is tearing down).
	stopOnce sync.Once

	// query holds the annotated statement from the most recent RowsQueryEvent
	// so it can be attached to the following RowsEvent (marginalia detection).
	// canal delivers OnRowsQueryEvent before OnRow, so this is single-writer
	// within the canal callback goroutine.
	query []byte
}

func (s *BinlogStreamer) ensureLogger() {
	if s.LogTag == "" {
		s.LogTag = "binlog_streamer"
	}

	if s.logger == nil {
		s.logger = LogWithField("tag", s.LogTag)
	}
}

// coordinateMode returns the effective coordinate mode, treating the empty
// value as file/position for backwards compatibility.
func (s *BinlogStreamer) coordinateMode() BinlogCoordinateType {
	if s.BinlogCoordinateMode == "" {
		return BinlogCoordinateFilePosition
	}
	return s.BinlogCoordinateMode
}

// createCanal builds the canal instance. Unlike canal.NewDefaultConfig, dump
// mode is explicitly disabled (no ExecutionPath): Ghostferry only streams the
// binlog and does its own initial copy. Table filtering is left to Ghostferry's
// TableSchemaCache/Filter rather than canal's include/exclude regex, to keep
// behavior identical to the previous implementation.
func (s *BinlogStreamer) createCanal() error {
	var err error
	if s.MyServerId == 0 {
		s.MyServerId, err = s.generateNewServerId()
		if err != nil {
			s.logger.WithError(err).Error("could not generate unique server_id")
			return err
		}
	}

	cfg := canal.NewDefaultConfig()
	cfg.ServerID = s.MyServerId
	cfg.Flavor = mysql.MySQLFlavor
	cfg.User = s.DBConfig.User
	cfg.Password = s.DBConfig.Pass
	cfg.UseDecimal = true
	cfg.ParseTime = false
	cfg.TimestampStringLocation = time.UTC
	cfg.Logger = NewSlogLogger(s.logger)
	// Disable mysqldump: Ghostferry streams binlog only.
	cfg.Dump.ExecutionPath = ""
	// canal owns the event loop, so Ghostferry stops the stream by calling
	// canal.Close(). Close() must interrupt the syncer goroutine, which
	// otherwise parks in a blocking socket read with no data (e.g. after the
	// source goes read-only at cutover). A ReadTimeout wakes that read
	// periodically so Close() (which waits on the syncer goroutine) cannot
	// deadlock, and a HeartbeatPeriod keeps an otherwise-idle connection alive.
	cfg.HeartbeatPeriod = 1 * time.Second

	if s.DBConfig.Net == "unix" {
		cfg.Addr = s.DBConfig.Host
	} else {
		cfg.Addr = fmt.Sprintf("%s:%d", s.DBConfig.Host, s.DBConfig.Port)
	}

	if s.DBConfig.TLS != nil {
		cfg.TLSConfig, err = s.DBConfig.TLS.BuildConfig()
		if err != nil {
			return err
		}
	}

	s.canal, err = canal.NewCanal(cfg)
	if err != nil {
		return err
	}
	s.canal.SetEventHandler(&binlogEventHandler{streamer: s})
	return nil
}

// ConnectBinlogStreamerToMysql starts streaming from the current server
// file/position coordinate.
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

	if err := s.createCanal(); err != nil {
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

	return s.lastStreamedBinlogPosition, nil
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
	if err := s.createCanal(); err != nil {
		return BinlogCoordinate{}, err
	}

	gtidSet, err := startFrom.ParsedGTIDSet()
	if err != nil {
		s.logger.WithError(err).Error("failed to parse starting GTID set")
		return BinlogCoordinate{}, err
	}

	// Seed both streamed and resumable GTID sets to the starting set. Clone so
	// later mutations from event tracking never alias the starting value.
	s.lastStreamedGTIDSet = gtidSet.Clone()
	s.lastResumableGTIDSet = gtidSet.Clone()

	s.logger.WithFields(Fields{
		"gtid_set": gtidSet.String(),
		"host":     s.DBConfig.Host,
		"port":     s.DBConfig.Port,
	}).Info("starting binlog streaming from GTID set")

	return NewGTIDCoordinate(s.lastStreamedGTIDSet.String()), nil
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

// Run drives the canal until the stop coordinate is reached. canal owns the
// low-level syncer, event loop, reconnect logic, position tracking and DDL
// parsing; Ghostferry only observes events through binlogEventHandler and
// decides when to stop.
func (s *BinlogStreamer) Run() {
	s.ensureLogger()

	defer func() {
		s.logger.WithFields(Fields{
			"stopAtBinlogPosition":       s.stopAtBinlogPosition,
			"lastStreamedBinlogPosition": s.lastStreamedBinlogPosition,
			"coordinateMode":             s.coordinateMode(),
		}).Info("exiting binlog streamer")
		s.closeCanal()
	}()

	s.logger.Info("starting binlog streamer")

	// canal owns the event loop and has no built-in "stop at coordinate", so
	// Ghostferry's cutover semantics live in this background monitor. It also
	// preserves the previous streamer's idle keep-alive: while streaming, it
	// advances lastProcessedEventTime every tick so IsAlmostCaughtUp stays true
	// on an idle source (matching the old GetEvent 500ms timeout behavior). Once
	// the stop coordinate is reached it closes the canal exactly once, which
	// unblocks RunFrom/StartFromGTID below.
	stopMonitorDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopMonitorDone:
				return
			case <-ticker.C:
				s.lastProcessedEventTime = time.Now()
				if !s.shouldContinueStreaming() {
					s.closeCanal()
					return
				}
			}
		}
	}()

	var err error
	switch s.coordinateMode() {
	case BinlogCoordinateGTID:
		err = s.canal.StartFromGTID(s.lastStreamedGTIDSet)
	default:
		err = s.canal.RunFrom(s.lastStreamedBinlogPosition)
	}

	close(stopMonitorDone)

	// A deliberate stop closes the canal, which surfaces as a context.Canceled
	// / closed error from RunFrom. Only treat it as fatal if we were not
	// stopping.
	if err != nil && !s.stopRequested {
		s.ErrorHandler.Fatal("binlog_streamer", err)
	}
}

// closeCanal closes the underlying canal exactly once. It is safe to call from
// both the stop monitor goroutine and Run's defer.
func (s *BinlogStreamer) closeCanal() {
	s.stopOnce.Do(func() {
		if s.canal != nil {
			s.canal.Close()
		}
	})
}

func (s *BinlogStreamer) AddEventListener(listener func([]DMLEvent) error) {
	s.eventListeners = append(s.eventListeners, listener)
}

func (s *BinlogStreamer) GetLastStreamedBinlogPosition() mysql.Position {
	return s.lastStreamedBinlogPosition
}

// GetLastStreamedBinlogCoordinate is the coordinate-typed counterpart of
// GetLastStreamedBinlogPosition. It returns a coordinate matching the
// streamer's configured BinlogCoordinateMode.
func (s *BinlogStreamer) GetLastStreamedBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID {
		if s.lastStreamedGTIDSet == nil {
			return NewGTIDCoordinate("")
		}
		return NewGTIDCoordinate(s.lastStreamedGTIDSet.String())
	}
	return NewFilePositionCoordinate(s.lastStreamedBinlogPosition)
}

// GetStopBinlogCoordinate returns the recorded stop coordinate matching the
// streamer's configured BinlogCoordinateMode. It is zero until FlushAndStop has
// recorded a stop target.
func (s *BinlogStreamer) GetStopBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID {
		if s.stopAtGTIDSet == nil {
			return NewGTIDCoordinate("")
		}
		return NewGTIDCoordinate(s.stopAtGTIDSet.String())
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
			s.stopAtGTIDSet = parsed
			return nil
		})

		if err != nil {
			s.ErrorHandler.Fatal("binlog_streamer", err)
		}
		s.logger.WithField("stop_at_gtid_set", s.stopAtGTIDSet.String()).Info("current stop GTID set was recorded")

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

// updateLag emits the replication lag metric, throttled to once per second. The
// event time is derived from the binlog event header timestamp.
func (s *BinlogStreamer) updateLag(evTimestamp uint32) {
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

// handleRowsEvent converts a canal RowsEvent into Ghostferry DMLEvents, applies
// rewrites/filtering, stamps coordinates, and fans out to listeners.
func (s *BinlogStreamer) handleRowsEvent(e *canal.RowsEvent) error {
	pos := mysql.Position{
		Name: s.lastStreamedBinlogPosition.Name,
		Pos:  e.Header.LogPos,
	}

	db := string(e.Table.Schema)
	if rewrittenDBName, exists := s.DatabaseRewrites[db]; exists {
		db = rewrittenDBName
	}

	table := string(e.Table.Name)
	if rewrittenTableName, exists := s.TableRewrites[table]; exists {
		table = rewrittenTableName
	}

	tableFromSchemaCache := s.TableSchema.Get(db, table)
	if tableFromSchemaCache == nil {
		return nil
	}

	dmlEvs, err := NewBinlogDMLEventsFromCanal(tableFromSchemaCache, e, pos, s.lastResumableBinlogPosition, s.query)
	if err != nil {
		return err
	}

	// In GTID mode, stamp GTID coordinates onto the events so that downstream
	// consumers (binlog writer, verifiers) advance GTID-based state rather than
	// file/position. The resumable coordinate is the committed set BEFORE the
	// current transaction, so an interruption replays the whole transaction.
	if s.coordinateMode() == BinlogCoordinateGTID {
		currentCoord := NewGTIDCoordinateFromSet(s.lastStreamedGTIDSet)
		resumableCoord := NewGTIDCoordinateFromSet(s.lastResumableGTIDSet)
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
		if err := listener(events); err != nil {
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

// binlogEventHandler implements canal.EventHandler. It is the single adapter
// between canal's callback API and Ghostferry's DMLEvent/coordinate model.
// This replaces the previous hand-rolled event loop and big event-type switch.
type binlogEventHandler struct {
	canal.DummyEventHandler
	streamer *BinlogStreamer
}

func (h *binlogEventHandler) OnRotate(header *replication.EventHeader, e *replication.RotateEvent) error {
	s := h.streamer
	s.lastStreamedBinlogPosition = mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}
	s.logger.WithFields(Fields{
		"new_position": s.lastStreamedBinlogPosition.Pos,
		"new_filename": s.lastStreamedBinlogPosition.Name,
	}).Info("binlog file rotated")
	return nil
}

func (h *binlogEventHandler) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	// A RowsQueryEvent always precedes the corresponding RowsEvent when
	// binlog_rows_query_log_events=ON. It carries the full query (with
	// annotations/marginalia) so downstream can attach it to the row events.
	h.streamer.query = e.Query
	return nil
}

func (h *binlogEventHandler) OnRow(e *canal.RowsEvent) error {
	s := h.streamer
	s.updateLag(e.Header.Timestamp)
	if e.Header.LogPos != 0 {
		s.lastStreamedBinlogPosition.Pos = e.Header.LogPos
	}
	if err := s.handleRowsEvent(e); err != nil {
		s.logger.WithError(err).Error("failed to handle rows event")
		s.ErrorHandler.Fatal("binlog_streamer", err)
	}
	return nil
}

func (h *binlogEventHandler) OnGTID(header *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error {
	s := h.streamer
	s.updateLag(header.Timestamp)
	if s.coordinateMode() != BinlogCoordinateGTID {
		return nil
	}
	// Start of a transaction. A safe resume point is the committed set that
	// existed BEFORE this transaction, so an interruption replays the whole
	// in-flight transaction.
	if s.lastStreamedGTIDSet != nil {
		s.lastResumableGTIDSet = s.lastStreamedGTIDSet.Clone()
	}
	return nil
}

func (h *binlogEventHandler) OnXID(header *replication.EventHeader, pos mysql.Position) error {
	s := h.streamer
	s.updateLag(header.Timestamp)
	s.lastResumableBinlogPosition = mysql.Position{Name: s.lastStreamedBinlogPosition.Name, Pos: pos.Pos}
	if pos.Pos != 0 {
		s.lastStreamedBinlogPosition.Pos = pos.Pos
	}
	// End of a transaction: advance the committed GTID set. canal maintains the
	// executed set internally and exposes it via SyncedGTIDSet.
	if s.coordinateMode() == BinlogCoordinateGTID {
		s.advanceStreamedGTID(s.canal.SyncedGTIDSet())
	}
	// A new RowsQueryEvent will set the query before the next RowsEvent.
	s.query = nil
	return nil
}

func (h *binlogEventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, e *replication.QueryEvent) error {
	s := h.streamer
	s.updateLag(header.Timestamp)
	if nextPos.Pos != 0 {
		s.lastStreamedBinlogPosition.Pos = nextPos.Pos
	}

	// A DDL/admin statement commits as its own transaction without an XIDEvent,
	// so advance the committed GTID set here too. Skip transaction-control
	// statements, which commit at their XIDEvent.
	if s.coordinateMode() == BinlogCoordinateGTID && !isTransactionControlQuery(e.Query) {
		s.advanceStreamedGTID(s.canal.SyncedGTIDSet())
	}

	if s.DDLEventHandler != nil {
		return s.DDLEventHandler(string(e.Schema), "", e.Query)
	}
	return nil
}

// advanceStreamedGTID advances the committed (streamed) GTID set to committed,
// recording the previous committed set as the resumable point first so that an
// interruption replays the whole just-committed transaction. It is a no-op when
// committed is nil. Extracted so the transaction-boundary GTID logic is unit
// testable without a live canal.
func (s *BinlogStreamer) advanceStreamedGTID(committed mysql.GTIDSet) {
	if committed == nil {
		return
	}
	if s.lastStreamedGTIDSet != nil {
		s.lastResumableGTIDSet = s.lastStreamedGTIDSet.Clone()
	}
	s.lastStreamedGTIDSet = committed.Clone()
}

func (h *binlogEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	if pos.Name != "" {
		h.streamer.lastStreamedBinlogPosition.Name = pos.Name
	}
	return nil
}

func (h *binlogEventHandler) String() string {
	return "ghostferryBinlogEventHandler"
}
