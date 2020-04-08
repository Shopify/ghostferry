package ghostferry

import (
	"context"
	"crypto/tls"
	sqlorig "database/sql"
	"fmt"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/sirupsen/logrus"
)

const caughtUpThreshold = 10 * time.Second

type BinlogStreamer struct {
	DB           *sql.DB
	DBConfig     *DatabaseConfig
	MyServerId   uint32
	ErrorHandler ErrorHandler
	Filter       CopyFilter

	TableSchema TableSchemaCache

	binlogSyncer                *replication.BinlogSyncer
	binlogStreamer              *replication.BinlogStreamer
	lastStreamedBinlogPosition  mysql.Position
	lastResumableBinlogPosition mysql.Position
	targetBinlogPosition        mysql.Position
	lastProcessedEventTime      time.Time
	lastLagMetricEmittedTime    time.Time

	stopRequested bool

	logger         *logrus.Entry
	eventListeners []func([]DMLEvent) error
}

func (s *BinlogStreamer) ensureLogger() {
	if s.logger == nil {
		s.logger = logrus.WithField("tag", "binlog_streamer")
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

	s.logger.WithFields(logrus.Fields{
		"file": s.lastStreamedBinlogPosition.Name,
		"pos":  s.lastStreamedBinlogPosition.Pos,
	}).Info("starting binlog streaming")

	s.binlogStreamer, err = s.binlogSyncer.StartSync(s.lastStreamedBinlogPosition)
	if err != nil {
		s.logger.WithError(err).Error("unable to start binlog streamer")
		return mysql.Position{}, err
	}

	return s.lastStreamedBinlogPosition, err
}

func (s *BinlogStreamer) Run() {
	s.ensureLogger()

	defer func() {
		s.logger.Info("exiting binlog streamer")
		s.binlogSyncer.Close()
	}()

	s.logger.Info("starting binlog streamer")
	for !s.stopRequested || (s.stopRequested && s.lastStreamedBinlogPosition.Compare(s.targetBinlogPosition) < 0) {
		var ev *replication.BinlogEvent
		var timedOut bool

		err := WithRetries(5, 0, s.logger, "get binlog event", func() (er error) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			ev, er = s.binlogStreamer.GetEvent(ctx)

			if er == context.DeadlineExceeded {
				timedOut = true
				return nil
			}

			return er
		})

		if err != nil {
			s.ErrorHandler.Fatal("binlog_streamer", err)
		}

		if timedOut {
			s.lastProcessedEventTime = time.Now()
			continue
		}

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			// This event is needed because we need to update the last successful
			// binlog position.
			s.lastResumableBinlogPosition.Pos = uint32(e.Position)

			s.lastStreamedBinlogPosition.Pos = uint32(e.Position)
			s.lastStreamedBinlogPosition.Name = string(e.NextLogName)
			s.logger.WithFields(logrus.Fields{
				"pos":  s.lastStreamedBinlogPosition.Pos,
				"file": s.lastStreamedBinlogPosition.Name,
			}).Info("rotated binlog file")
		case *replication.TableMapEvent:
			continue
		case *replication.RowsEvent:
			err = s.handleRowsEvent(ev)
			if err != nil {
				s.logger.WithError(err).Error("failed to handle rows event")
				s.ErrorHandler.Fatal("binlog_streamer", err)
			}
			s.updateLastStreamedPosAndTime(ev)
		case *replication.FormatDescriptionEvent:
			// This event has a LogPos = 0, presumably because this is the first
			// event received by the BinlogStreamer to get some metadata about
			// how the binlog is supposed to be transmitted.
			// We don't want to save the binlog position derived from this event
			// as it will contain the wrong thing.
			continue
			// case *replication.QueryEvent:
			// This event can tell us about table structure change which means
			// the cached schemas of the tables would be invalidated.
			// TODO: investigate using this to allow for migrations to occur.
		case *replication.GenericEvent:
			// go-mysql don't parse all events and unparsed events are denoted
			// with empty GenericEvent structs.
			// so there's no way to handle this for us.
			continue
		default:
			s.lastResumableBinlogPosition.Pos = uint32(ev.Header.LogPos)
			s.updateLastStreamedPosAndTime(ev)
		}
	}
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
		s.targetBinlogPosition, err = ShowMasterStatusBinlogPosition(s.DB)
		return err
	})

	if err != nil {
		s.ErrorHandler.Fatal("binlog_streamer", err)
	}
	s.logger.WithField("target_position", s.targetBinlogPosition).Info("current stop binlog position was recorded")

	s.stopRequested = true
}

func (s *BinlogStreamer) updateLastStreamedPosAndTime(ev *replication.BinlogEvent) {
	if ev.Header.LogPos == 0 || ev.Header.Timestamp == 0 {
		// This shouldn't happen, as the cases where it does happen are excluded.
		// However, I've not seen all the cases yet.
		s.logger.Panicf("logpos: %d %d %T", ev.Header.LogPos, ev.Header.Timestamp, ev.Event)
	}

	s.lastStreamedBinlogPosition.Pos = ev.Header.LogPos
	eventTime := time.Unix(int64(ev.Header.Timestamp), 0)
	s.lastProcessedEventTime = eventTime

	if time.Since(s.lastLagMetricEmittedTime) >= time.Second {
		lag := time.Since(eventTime)
		metrics.Gauge("BinlogStreamer.Lag", lag.Seconds(), nil, 1.0)
		s.lastLagMetricEmittedTime = time.Now()
	}
}

func (s *BinlogStreamer) handleRowsEvent(ev *replication.BinlogEvent) error {
	eventTime := time.Unix(int64(ev.Header.Timestamp), 0)
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

	table := s.TableSchema.Get(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
	if table == nil {
		return nil
	}

	s.lastResumableBinlogPosition.Name = s.lastStreamedBinlogPosition.Name
	dmlEvs, err := NewBinlogDMLEvents(table, ev, pos, s.lastResumableBinlogPosition)
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
		s.logger.WithFields(logrus.Fields{
			"database": dmlEv.Database(),
			"table":    dmlEv.Table(),
		}).Debugf("received event %T at %v", dmlEv, eventTime)

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

		err = rows.Scan(&server_id, &host, &port, &master_id, &slave_uuid)
		if err != nil {
			return nil, fmt.Errorf("could not scan SHOW SLAVE HOSTS row, err: %s", err.Error())
		}

		server_ids = append(server_ids, server_id)
	}

	return server_ids, nil
}
