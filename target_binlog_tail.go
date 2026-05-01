package ghostferry

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/replication"
)

// TargetBinlogTail is a lightweight binlog reader pointed at the target
// database. Its sole purpose is to surface DDL events that land on the target
// — for example a tier-scheduled ALTER, or a gh-ost shadow-table CREATE — so
// the schema-change detector can pair them with source-side DDL and trigger a
// recopy when both sides converge.
//
// Unlike the main BinlogStreamer the target tail does not parse row events
// and does not feed downstream listeners. RowEvents are dropped on the floor.
type TargetBinlogTail struct {
	DB           *sql.DB
	DBConfig     *DatabaseConfig
	MyServerId   uint32
	ErrorHandler ErrorHandler

	// OnDDL is invoked for every successfully parsed DDL QueryEvent observed
	// on the target. Non-DDL QUERY_EVENTs (BEGIN, COMMIT, SAVEPOINT) are
	// dropped before the callback fires.
	OnDDL func(*DDLStatement)

	binlogSyncer   *replication.BinlogSyncer
	binlogStreamer *replication.BinlogStreamer

	stopRequested bool
	logger        Logger
}

func (t *TargetBinlogTail) ensureLogger() {
	if t.logger == nil {
		t.logger = LogWithField("tag", "target_binlog_tail")
	}
}

func (t *TargetBinlogTail) connect() error {
	t.ensureLogger()

	var tlsConfig *tls.Config
	if t.DBConfig != nil && t.DBConfig.TLS != nil {
		var err error
		tlsConfig, err = t.DBConfig.TLS.BuildConfig()
		if err != nil {
			return err
		}
	}

	if t.MyServerId == 0 {
		// The target tail must use a server_id distinct from any source-side
		// reader. Caller is expected to either set this explicitly or accept a
		// random one — collisions are exceedingly unlikely against a target
		// shop pod which is not itself a replication source.
		t.MyServerId = randomServerId()
	}

	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:                 t.MyServerId,
		Host:                     t.DBConfig.Host,
		Port:                     t.DBConfig.Port,
		User:                     t.DBConfig.User,
		Password:                 t.DBConfig.Pass,
		TLSConfig:                tlsConfig,
		UseDecimal:               true,
		UseFloatWithTrailingZero: true,
		TimestampStringLocation:  time.UTC,
		Logger:                   NewSlogLogger(t.logger),
	}
	t.binlogSyncer = replication.NewBinlogSyncer(syncerConfig)

	startPos, err := ShowMasterStatusBinlogPosition(t.DB)
	if err != nil {
		t.logger.WithError(err).Error("failed to read target binlog position")
		return err
	}

	t.logger.WithFields(Fields{
		"file":     startPos.Name,
		"position": startPos.Pos,
		"host":     t.DBConfig.Host,
		"port":     t.DBConfig.Port,
	}).Info("starting target binlog tail")

	t.binlogStreamer, err = t.binlogSyncer.StartSync(startPos)
	if err != nil {
		t.logger.WithError(err).Error("unable to start target binlog tail")
		return err
	}
	return nil
}

// Run streams the target's binlog and dispatches DDL QueryEvents to OnDDL
// until ctx is cancelled or Stop is called. Connection failures are fatal —
// the design treats schema-drift detection as load-bearing for correctness, so
// silently degrading the tail would let a target-side ALTER slip past while
// source rows continued to apply.
func (t *TargetBinlogTail) Run(ctx context.Context) error {
	t.ensureLogger()

	if t.OnDDL == nil {
		return errors.New("TargetBinlogTail.OnDDL must be set before Run")
	}

	if err := t.connect(); err != nil {
		t.ErrorHandler.Fatal("target_binlog_tail", err)
		return err
	}
	defer t.binlogSyncer.Close()

	for !t.stopRequested {
		select {
		case <-ctx.Done():
			t.logger.Info("target binlog tail stopping (context cancelled)")
			return nil
		default:
		}

		ev, err := t.readEvent(ctx)
		if err == context.DeadlineExceeded {
			continue
		}
		if err == context.Canceled {
			return nil
		}
		if err != nil {
			t.logger.WithError(err).Error("target binlog tail read failed")
			t.ErrorHandler.Fatal("target_binlog_tail", err)
			return err
		}

		t.handleEvent(ev)
	}
	return nil
}

func (t *TargetBinlogTail) readEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	readCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	return t.binlogStreamer.GetEvent(readCtx)
}

func (t *TargetBinlogTail) handleEvent(ev *replication.BinlogEvent) {
	queryEv, ok := ev.Event.(*replication.QueryEvent)
	if !ok {
		return
	}
	if !IsDDLQuery(queryEv.Query) {
		return
	}
	stmt := ParseDDLFromQueryEvent(queryEv)
	if stmt == nil {
		return
	}

	t.logger.WithFields(Fields{
		"schema":  stmt.SchemaName,
		"table":   stmt.TableName,
		"ddlType": stmt.DDLType,
	}).Info("target DDL observed")

	defer func() {
		if r := recover(); r != nil {
			t.logger.WithField("panic", fmt.Sprintf("%v", r)).Error("OnDDL handler panicked; continuing")
		}
	}()
	t.OnDDL(stmt)
}

// Stop signals the tail loop to exit at the next iteration.
func (t *TargetBinlogTail) Stop() {
	t.stopRequested = true
}
