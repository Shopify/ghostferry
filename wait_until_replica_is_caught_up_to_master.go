package ghostferry

import (
	"errors"
	"math"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type ReplicatedMasterPositionFetcher interface {
	Current(*sql.DB) (mysql.Position, error)
}

// ReplicatedMasterCoordinateFetcher is the coordinate-typed counterpart of
// ReplicatedMasterPositionFetcher. Implementations return the replication
// coordinate (file/position or GTID) that the replica has replicated up to.
type ReplicatedMasterCoordinateFetcher interface {
	CurrentCoordinate(*sql.DB) (BinlogCoordinate, error)
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

	return NewMysqlPosition(file, pos, err, replicaDB)
}

// CurrentCoordinate adapts the file/position fetcher to the coordinate API.
func (r ReplicatedMasterPositionViaCustomQuery) CurrentCoordinate(replicaDB *sql.DB) (BinlogCoordinate, error) {
	pos, err := r.Current(replicaDB)
	if err != nil {
		return BinlogCoordinate{}, err
	}
	return NewFilePositionCoordinate(pos), nil
}

// ReplicatedMasterGTIDViaCustomQuery selects the GTID set the replica has
// replicated up to from a heartbeat table of sort.
type ReplicatedMasterGTIDViaCustomQuery struct {
	// The custom query must return a single row with a single column: the
	// master's executed GTID set as a string, e.g.
	// "SELECT gtid_executed FROM meta.ptheartbeat WHERE server_id = %d".
	//
	// As with the file/position variant, do not use the replica's own applied
	// GTID set if it differs from the master's executed set; return the master's
	// set that has been durably replicated.
	Query string
}

// CurrentCoordinate returns the replicated master GTID set as a coordinate.
func (r ReplicatedMasterGTIDViaCustomQuery) CurrentCoordinate(replicaDB *sql.DB) (BinlogCoordinate, error) {
	var gtidSet string
	row := replicaDB.QueryRow(r.Query)
	if err := row.Scan(&gtidSet); err != nil {
		return BinlogCoordinate{}, err
	}
	if _, err := mysql.ParseMysqlGTIDSet(gtidSet); err != nil {
		return BinlogCoordinate{}, err
	}
	return NewGTIDCoordinate(gtidSet), nil
}

// Only set the MasterDB and ReplicatedMasterPosition options in your code as
// the others will be overwritten by the ferry.
type WaitUntilReplicaIsCaughtUpToMaster struct {
	MasterDB                        *sql.DB
	ReplicatedMasterPositionFetcher ReplicatedMasterPositionFetcher

	// ReplicatedMasterCoordinateFetcher is the coordinate-typed fetcher. When
	// set it takes precedence over ReplicatedMasterPositionFetcher. In GTID
	// mode this must be set (e.g. to a ReplicatedMasterGTIDViaCustomQuery).
	ReplicatedMasterCoordinateFetcher ReplicatedMasterCoordinateFetcher

	// BinlogCoordinateMode selects how the target master coordinate is read and
	// compared. Empty means file/position for backwards compatibility.
	BinlogCoordinateMode BinlogCoordinateType

	Timeout time.Duration

	ReplicaDB *sql.DB

	logger Logger
}

func (w *WaitUntilReplicaIsCaughtUpToMaster) coordinateMode() BinlogCoordinateType {
	if w.BinlogCoordinateMode == "" {
		return BinlogCoordinateFilePosition
	}
	return w.BinlogCoordinateMode
}

// coordinateFetcher returns the coordinate-typed fetcher, adapting the legacy
// file/position fetcher when only that is set.
func (w *WaitUntilReplicaIsCaughtUpToMaster) coordinateFetcher() ReplicatedMasterCoordinateFetcher {
	if w.ReplicatedMasterCoordinateFetcher != nil {
		return w.ReplicatedMasterCoordinateFetcher
	}
	if w.ReplicatedMasterPositionFetcher != nil {
		// ReplicatedMasterPositionViaCustomQuery already implements the
		// coordinate interface; other custom fetchers are wrapped here.
		if cf, ok := w.ReplicatedMasterPositionFetcher.(ReplicatedMasterCoordinateFetcher); ok {
			return cf
		}
		return positionFetcherAdapter{w.ReplicatedMasterPositionFetcher}
	}
	return nil
}

type positionFetcherAdapter struct {
	fetcher ReplicatedMasterPositionFetcher
}

func (a positionFetcherAdapter) CurrentCoordinate(replicaDB *sql.DB) (BinlogCoordinate, error) {
	pos, err := a.fetcher.Current(replicaDB)
	if err != nil {
		return BinlogCoordinate{}, err
	}
	return NewFilePositionCoordinate(pos), nil
}

// IsCaughtUp reports whether the replica has replicated up to the given
// file/position target. It is retained for backwards compatibility; new code
// should prefer IsCaughtUpToCoordinate.
func (w *WaitUntilReplicaIsCaughtUpToMaster) IsCaughtUp(targetMasterPos mysql.Position, retries int) (bool, error) {
	return w.IsCaughtUpToCoordinate(NewFilePositionCoordinate(targetMasterPos), retries)
}

// IsCaughtUpToCoordinate reports whether the replica has replicated up to the
// given target coordinate. For file/position it compares positions; for GTID
// it checks that the replica's executed set contains the target set.
func (w *WaitUntilReplicaIsCaughtUpToMaster) IsCaughtUpToCoordinate(targetMaster BinlogCoordinate, retries int) (bool, error) {
	if w.logger == nil {
		w.logger = LogWithField("tag", "wait_replica")
	}

	fetcher := w.coordinateFetcher()
	if fetcher == nil {
		return false, errors.New("no replicated master coordinate fetcher configured")
	}

	var current BinlogCoordinate
	err := WithRetries(retries, 600*time.Millisecond, w.logger, "read replicated master coordinate", func() error {
		var err error
		current, err = fetcher.CurrentCoordinate(w.ReplicaDB)
		return err
	})

	if err != nil {
		return false, err
	}

	if targetMaster.IsGTID() {
		contains, err := current.Contains(targetMaster)
		if err != nil {
			return false, err
		}
		if contains {
			w.logger.Infof("target master GTID set reached by replica: %v contains %v\n", current, targetMaster)
			return true, nil
		}
		w.logger.Debugf("replicated master GTID set %v does not yet contain %v\n", current, targetMaster)
		return false, nil
	}

	if current.Compare(targetMaster) >= 0 {
		w.logger.Infof("target master position reached by replica: %v >= %v\n", current, targetMaster)
		return true, nil
	}

	w.logger.Debugf("replicated master position is: %v < %v\n", current, targetMaster)
	return false, nil
}

func (w *WaitUntilReplicaIsCaughtUpToMaster) Wait() error {
	w.logger = LogWithField("tag", "wait_replica")
	// Essentially not timeout
	if w.Timeout == time.Duration(0) {
		w.Timeout = time.Duration(math.MaxInt64)
	}

	start := time.Now()

	var targetMaster BinlogCoordinate
	err := WithRetries(100, 600*time.Millisecond, w.logger, "read master coordinate", func() error {
		var err error
		if w.coordinateMode() == BinlogCoordinateGTID {
			targetMaster, err = ReadCurrentGTIDCoordinate(w.MasterDB)
			return err
		}
		var pos mysql.Position
		pos, err = ShowMasterStatusBinlogPosition(w.MasterDB)
		targetMaster = NewFilePositionCoordinate(pos)
		return err
	})

	if err != nil {
		w.logger.WithError(err).Error("failed to get master binlog coordinates")
		return err
	}

	w.logger.Infof("target master coordinate is: %v\n", targetMaster)

	for {
		isCaughtUp, err := w.IsCaughtUpToCoordinate(targetMaster, 100)
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
