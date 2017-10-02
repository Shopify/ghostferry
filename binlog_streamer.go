package ghostferry

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/sirupsen/logrus"
)

const caughtUpThreshold = 10 * time.Second

type BinlogStreamer struct {
	Db           *sql.DB
	Config       *Config
	ErrorHandler *ErrorHandler
	Throttler    *Throttler

	EventFilter DMLEventFilter

	binlogSyncer               *replication.BinlogSyncer
	binlogStreamer             *replication.BinlogStreamer
	lastStreamedBinlogPosition mysql.Position
	targetBinlogPosition       mysql.Position
	lastProcessedEventTime     time.Time

	stopRequested bool

	logger         *logrus.Entry
	eventListeners []func([]DMLEvent) error
}

func (s *BinlogStreamer) Initialize() (err error) {
	s.logger = logrus.WithField("tag", "binlog_streamer")
	var tlsConfig *tls.Config
	if s.Config.SourceTLS != nil {
		tlsConfig, err = s.Config.SourceTLS.BuildConfig()
		if err != nil {
			return err
		}
	}

	syncerConfig := &replication.BinlogSyncerConfig{
		ServerID:  s.Config.MyServerId,
		Host:      s.Config.SourceHost,
		Port:      s.Config.SourcePort,
		User:      s.Config.SourceUser,
		Password:  s.Config.SourcePass,
		TLSConfig: tlsConfig,
		LogLevel:  "warn",
	}

	s.binlogSyncer = replication.NewBinlogSyncer(syncerConfig)
	s.stopRequested = false
	return nil
}

func (s *BinlogStreamer) ConnectBinlogStreamerToMysql() error {
	s.logger.Info("reading current binlog position")
	var err error
	s.lastStreamedBinlogPosition, err = s.readCurrentBinlogPositionFromMasterStatus()
	if err != nil {
		s.logger.WithError(err).Error("failed to read current binlog position")
		return err
	}

	s.logger.WithFields(logrus.Fields{
		"file": s.lastStreamedBinlogPosition.Name,
		"pos":  s.lastStreamedBinlogPosition.Pos,
	}).Info("found binlog position, starting synchronization")

	s.binlogStreamer, err = s.binlogSyncer.StartSync(s.lastStreamedBinlogPosition)
	if err != nil {
		s.logger.WithError(err).Error("unable to start binlog streamer")
		return err
	}

	return nil
}

func (s *BinlogStreamer) Run(wg *sync.WaitGroup) {
	defer func() {
		s.logger.Info("exiting binlog streamer")
		s.binlogSyncer.Close()
		wg.Done()
	}()

	s.logger.Info("starting binlog streamer")

	for !s.stopRequested || (s.stopRequested && s.lastStreamedBinlogPosition.Compare(s.targetBinlogPosition) < 0) {
		s.Throttler.ThrottleIfNecessary()

		ctx, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)
		ev, err := s.binlogStreamer.GetEvent(ctx)
		if err != nil {
			if err == context.DeadlineExceeded {
				s.lastProcessedEventTime = time.Now()
				continue
			}

			s.ErrorHandler.Fatal("binlog_streamer", err)
			return
		}

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			// This event is needed because we need to update the last successful
			// binlog position.
			s.lastStreamedBinlogPosition.Pos = uint32(e.Position)
			s.lastStreamedBinlogPosition.Name = string(e.NextLogName)
			s.logger.WithFields(logrus.Fields{
				"pos":  s.lastStreamedBinlogPosition.Pos,
				"file": s.lastStreamedBinlogPosition.Name,
			}).Info("rotated binlog file")
		case *replication.RowsEvent:
			err = s.handleRowsEvent(ev)
			if err != nil {
				s.logger.WithError(err).Error("failed to handle rows event")
				s.ErrorHandler.Fatal("binlog_streamer", err)
				return
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
		default:
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
	var err error
	// Must first read the binlog position before requesting stop
	// Otherwise there is a race condition where the stopRequested is
	// set to True but the TargetPosition is nil, which would cause
	// the BinlogStreamer to immediately exit, as it thinks that it has
	// passed the initial target position.
	for {
		s.targetBinlogPosition, err = s.readCurrentBinlogPositionFromMasterStatus()
		if err == nil {
			break
		}

		s.logger.WithError(err).Error("failed to read current binlog position, retrying...")
		time.Sleep(500 * time.Millisecond)
	}

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
	if eventTime.Before(s.lastProcessedEventTime) {
		s.logger.Warnf("new event time is before the last event time: %v < %v", eventTime, s.lastProcessedEventTime)
	}

	s.lastProcessedEventTime = eventTime
}

func (s *BinlogStreamer) handleRowsEvent(ev *replication.BinlogEvent) error {
	eventTime := time.Unix(int64(ev.Header.Timestamp), 0)

	dmlEvs, err := NewBinlogDMLEvents(ev)
	if err != nil {
		return err
	}

	events := make([]DMLEvent, 0)

	for _, dmlEv := range dmlEvs {
		if len(FilterForApplicable([]string{dmlEv.Database()}, s.Config.ApplicableDatabases)) == 0 {
			continue
		}

		if len(FilterForApplicable([]string{dmlEv.Table()}, s.Config.ApplicableTables)) == 0 {
			continue
		}

		if s.EventFilter != nil && !s.EventFilter.Applicable(dmlEv) {
			continue
		}

		events = append(events, dmlEv)
		s.logger.WithFields(logrus.Fields{
			"database": dmlEv.Database(),
			"table":    dmlEv.Table(),
		}).Debugf("received event %T at %v", dmlEv, eventTime)
	}

	for _, listener := range s.eventListeners {
		err := listener(events)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *BinlogStreamer) readCurrentBinlogPositionFromMasterStatus() (mysql.Position, error) {
	query := "show master status"
	row := s.Db.QueryRow(query)
	var file string
	var position uint32
	var binlog_do_db, binlog_ignore_db, executed_gtid_set string
	err := row.Scan(&file, &position, &binlog_do_db, &binlog_ignore_db, &executed_gtid_set)

	switch {
	case err == sql.ErrNoRows:
		return mysql.Position{},
			fmt.Errorf("no results from show master status")
	case err != nil:
		return mysql.Position{}, err
	default:
		if file == "" {
			return mysql.Position{},
				fmt.Errorf("show master status does not show a file")
		}

		return mysql.Position{
			Name: file,
			Pos:  position,
		}, nil
	}
}
