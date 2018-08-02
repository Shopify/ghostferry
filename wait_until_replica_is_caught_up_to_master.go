package ghostferry

import (
	"database/sql"
	"errors"
	"math"
	"time"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

type ReplicatedMasterPositionFetcher interface {
	Current(*sql.DB) (mysql.Position, error)
}

// Selects the master position that we have replicated until from a heartbeat
// table of sort.
type ReplicatedMasterPositionViaCustomQuery struct {
	// The custom query executing should return a single row with two values:
	// the string file and the integer position. For pt-heartbeat, this query
	// would be:
	//
	// "SELECT file, position FROM meta.ptheartbeat WHERE server_id = %d" % serverId
	//
	// where serverId is the master server id, and meta.ptheartbeat is the table
	// where pt-heartbeat writes to.
	//
	// For pt-heartbeat in particular, you should not use the
	// relay_master_log_file and exec_master_log_pos of the DB being replicated
	// as these values are not the master binlog positions.
	Query string
}

func (r ReplicatedMasterPositionViaCustomQuery) Current(replicaDB *sql.DB) (mysql.Position, error) {
	var file string
	var pos uint32
	row := replicaDB.QueryRow(r.Query)
	err := row.Scan(&file, &pos)

	return NewMysqlPosition(file, pos, err)
}

// Only set the MasterDB and ReplicatedMasterPosition options in your code as
// the others will be overwritten by the ferry.
type WaitUntilReplicaIsCaughtUpToMaster struct {
	MasterDB                        *sql.DB
	ReplicatedMasterPositionFetcher ReplicatedMasterPositionFetcher
	Timeout                         time.Duration

	ReplicaDB *sql.DB

	logger *logrus.Entry
}

func (w *WaitUntilReplicaIsCaughtUpToMaster) IsCaughtUp(targetMasterPos mysql.Position, retries int) (bool, error) {
	if w.logger == nil {
		w.logger = logrus.WithField("tag", "wait_replica")
	}

	var currentReplicatedMasterPos mysql.Position
	err := WithRetries(retries, 600*time.Millisecond, w.logger, "read replicated master binlog position", func() error {
		var err error
		currentReplicatedMasterPos, err = w.ReplicatedMasterPositionFetcher.Current(w.ReplicaDB)
		return err
	})

	if err != nil {
		return false, err
	}

	if currentReplicatedMasterPos.Compare(targetMasterPos) >= 0 {
		w.logger.Infof("target master position reached by replica: %v >= %v\n", currentReplicatedMasterPos, targetMasterPos)
		return true, nil
	}

	w.logger.Debugf("replicated master position is: %v < %v\n", currentReplicatedMasterPos, targetMasterPos)
	return false, nil
}

func (w *WaitUntilReplicaIsCaughtUpToMaster) Wait() error {
	w.logger = logrus.WithField("tag", "wait_replica")
	// Essentially not timeout
	if w.Timeout == time.Duration(0) {
		w.Timeout = time.Duration(math.MaxInt64)
	}

	start := time.Now()

	var targetMasterPos mysql.Position
	err := WithRetries(100, 600*time.Millisecond, w.logger, "read master binlog position", func() error {
		var err error
		targetMasterPos, err = ShowMasterStatusBinlogPosition(w.MasterDB)
		return err
	})

	if err != nil {
		w.logger.WithError(err).Error("failed to get master binlog coordinates")
		return err
	}

	w.logger.Infof("target master position is: %v\n", targetMasterPos)

	for {
		isCaughtUp, err := w.IsCaughtUp(targetMasterPos, 100)
		if err != nil {
			w.logger.WithError(err).Error("failed to get replica binlog coordinates")
			return err
		}

		if isCaughtUp {
			break
		}

		timeTaken := time.Now().Sub(start)
		if timeTaken >= w.Timeout {
			return errors.New("timeout reached before replica is caught up to master")
		}

		time.Sleep(600 * time.Millisecond)
	}

	return nil
}
