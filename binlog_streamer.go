package ghostferry

import (
	"context"
	"crypto/tls"
	sqlorig "database/sql"
	"fmt"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"
)

const caughtUpThreshold = 10 * time.Second

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

	lastStreamedBinlogPosition  mysql.Position
	lastResumableBinlogPosition mysql.Position
	stopAtBinlogPosition        mysql.Position

	lastProcessedEventTime   time.Time
	lastLagMetricEmittedTime time.Time

	stopRequested bool

	logger         *logrus.Entry
	eventListeners []func([]DMLEvent) error
	eventHandlers  map[string]func(*replication.BinlogEvent, []byte, *BinlogEventState) ([]byte, error)
}

func (s *BinlogStreamer) ensureLogger() {
	if s.LogTag == "" {
		s.LogTag = "binlog_streamer"
	}

	if s.logger == nil {
		s.logger = logrus.WithField("tag", s.LogTag)
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
		ServerID:                s.MyServerId,
		Host:                    s.DBConfig.Host,
		Port:                    s.DBConfig.Port,
		User:                    s.DBConfig.User,
		Password:                s.DBConfig.Pass,
		TLSConfig:               tlsConfig,
		UseDecimal:              true,
		TimestampStringLocation: time.UTC,
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

	s.logger.WithFields(logrus.Fields{
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

		s.logger.WithFields(logrus.Fields{
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

func (s *BinlogStreamer) Run() {
	s.ensureLogger()

	defer func() {
		s.logger.WithFields(logrus.Fields{
			"stopAtBinlogPosition":       s.stopAtBinlogPosition,
			"lastStreamedBinlogPosition": s.lastStreamedBinlogPosition,
		}).Info("exiting binlog streamer")
		s.binlogSyncer.Close()
	}()

	var query []byte
	es := BinlogEventState{}

	currentFilename := s.lastStreamedBinlogPosition.Name
	es.nextFilename = s.lastStreamedBinlogPosition.Name
	s.logger.Info("starting binlog streamer")
	for !s.stopRequested || (s.stopRequested && s.lastStreamedBinlogPosition.Compare(s.stopAtBinlogPosition) < 0) {
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

		s.logger.WithFields(logrus.Fields{
			"position":                   es.evPosition.Pos,
			"file":                       es.evPosition.Name,
			"type":                       fmt.Sprintf("%T", ev.Event),
			"lastStreamedBinlogPosition": s.lastStreamedBinlogPosition,
		}).Debug("reached position")

		es.isEventPositionResumable = false
		es.isEventPositionValid = true

		eventTypeString := ev.Header.EventType.String()
		if handler, ok := s.eventHandlers[eventTypeString]; ok {
			query, err = handler(ev, query, &es)
			if err != nil {
				s.logger.WithError(err).Error("failed to handle event")
				s.ErrorHandler.Fatal("binlog_streamer", err)
			}
		} else {
			query, err = s.defaultEventHandler(ev, query, &es)
		}

		if es.isEventPositionValid {
			evType := fmt.Sprintf("%T", ev.Event)
			evTimestamp := ev.Header.Timestamp
			s.updateLastStreamedPosAndTime(evTimestamp, es.evPosition, evType, es.isEventPositionResumable)
		}
	}
}

func (s *BinlogStreamer) AddBinlogEventHandler(ev string, eh func(*replication.BinlogEvent, []byte, *BinlogEventState) ([]byte, error)) {
	if s.eventHandlers == nil {
		s.eventHandlers = make(map[string]func(*replication.BinlogEvent, []byte, *BinlogEventState) ([]byte, error))
	}
	s.eventHandlers[ev] = eh
}

func (s *BinlogStreamer) AddEventListener(listener func([]DMLEvent) error) {
	s.eventListeners = append(s.eventListeners, listener)
}

func (s *BinlogStreamer) GetLastStreamedBinlogPosition() mysql.Position {
	return s.lastStreamedBinlogPosition
}

func (s *BinlogStreamer) IsAlmostCaughtUp() bool {
	return time.Now().Sub(s.lastProcessedEventTime) < caughtUpThreshold
}

func (s *BinlogStreamer) FlushAndStop() {
	s.logger.Info("requesting binlog streamer to stop")
	// Must first read the binlog position before requesting stop
	// Otherwise there is a race condition where the stopRequested is
	// set to True but the TargetPosition is nil, which would cause
	// the BinlogStreamer to immediately exit, as it thinks that it has
	// passed the initial target position.
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
	rows, err := db.Query("SHOW SLAVE HOSTS")
	if err != nil {
		return nil, fmt.Errorf("could not get slave hosts: %s", err)
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
			return nil, fmt.Errorf("could not get columns from slave hosts: %v", err)
		} else if len(columns) == 5 {
			err = rows.Scan(&server_id, &host, &port, &master_id, &slave_uuid)
		} else if len(columns) == 4 {
			err = rows.Scan(&server_id, &host, &port, &master_id)
		} else {
			return nil, fmt.Errorf("could not scan SHOW SLAVE HOSTS row, err: unknown result set with %d columns: %v", len(columns), columns)
		}
		if err != nil {
			return nil, fmt.Errorf("could not scan SHOW SLAVE HOSTS row, err: %s", err.Error())
		}

		server_ids = append(server_ids, server_id)
	}

	return server_ids, nil
}
