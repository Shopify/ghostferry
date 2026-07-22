package ghostferry

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	gtidSetLower  = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-57"
	gtidSetTarget = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	gtidSetPast   = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-150"
)

func mustParseGTID(t *testing.T, s string) mysql.GTIDSet {
	t.Helper()
	set, err := mysql.ParseMysqlGTIDSet(s)
	require.NoError(t, err)
	return set
}

func TestIsTransactionControlQuery(t *testing.T) {
	assert.True(t, isTransactionControlQuery([]byte("BEGIN")))
	assert.True(t, isTransactionControlQuery([]byte("  begin  ")))
	assert.True(t, isTransactionControlQuery([]byte("COMMIT")))
	assert.True(t, isTransactionControlQuery([]byte("ROLLBACK")))
	assert.False(t, isTransactionControlQuery([]byte("CREATE TABLE t (id int)")))
	assert.False(t, isTransactionControlQuery([]byte("GRANT ALL ON *.* TO 'x'@'%'")))
}

// TestQueryEventAdvancesStreamedGTID verifies that a DDL/admin QueryEvent
// (which commits without an XIDEvent) advances the streamed GTID set, while a
// BEGIN QueryEvent does not. Without this, a cutover whose stop target includes
// a trailing DDL would hang forever.
func TestQueryEventAdvancesStreamedGTID(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	s.logger = LogWithField("tag", "test")

	ddlSet := mustParseGTID(t, gtidSetTarget)

	// A DDL QueryEvent carrying the executed set advances lastStreamedGTIDSet.
	ddlEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{LogPos: 100},
		Event:  &replication.QueryEvent{Query: []byte("CREATE TABLE t (id int)"), GSet: ddlSet},
	}
	es := &BinlogEventState{}
	_, err := s.defaultEventHandler(ddlEvent, nil, es)
	require.NoError(t, err)
	require.NotNil(t, s.lastStreamedGTIDSet)
	assert.Equal(t, gtidSetTarget, s.lastStreamedGTIDSet.String())

	// A BEGIN QueryEvent must NOT advance the streamed set.
	before := s.lastStreamedGTIDSet.String()
	beginEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{LogPos: 200},
		Event:  &replication.QueryEvent{Query: []byte("BEGIN"), GSet: mustParseGTID(t, gtidSetPast)},
	}
	_, err = s.defaultEventHandler(beginEvent, nil, es)
	require.NoError(t, err)
	assert.Equal(t, before, s.lastStreamedGTIDSet.String(), "BEGIN must not advance the streamed GTID set")
}

func TestCoordinateModeDefaultsToFilePosition(t *testing.T) {
	s := &BinlogStreamer{}
	assert.Equal(t, BinlogCoordinateFilePosition, s.coordinateMode())

	s.BinlogCoordinateMode = BinlogCoordinateGTID
	assert.Equal(t, BinlogCoordinateGTID, s.coordinateMode())
}

func TestShouldContinueStreaming_FilePosition(t *testing.T) {
	s := &BinlogStreamer{}

	// No stop requested: always continue.
	assert.True(t, s.shouldContinueStreaming())

	s.stopRequested = true
	s.stopAtBinlogPosition = mysql.Position{Name: "mysql-bin.000010", Pos: 100}

	// Streamed position behind stop: continue.
	s.lastStreamedBinlogPosition = mysql.Position{Name: "mysql-bin.000010", Pos: 50}
	assert.True(t, s.shouldContinueStreaming())

	// Streamed position reached stop: stop.
	s.lastStreamedBinlogPosition = mysql.Position{Name: "mysql-bin.000010", Pos: 100}
	assert.False(t, s.shouldContinueStreaming())
}

func TestShouldContinueStreaming_GTID(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}

	// No stop requested: always continue.
	assert.True(t, s.shouldContinueStreaming())

	s.stopRequested = true
	s.stopAtGTIDSet = mustParseGTID(t, gtidSetTarget)

	// No streamed set yet: keep going.
	assert.True(t, s.shouldContinueStreaming())

	// Streamed set does not yet contain target: continue.
	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetLower)
	assert.True(t, s.shouldContinueStreaming())

	// Streamed set exactly reaches target: stop.
	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetTarget)
	assert.False(t, s.shouldContinueStreaming())

	// Streamed set past target: stop.
	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetPast)
	assert.False(t, s.shouldContinueStreaming())
}

// TestShouldContinueStreaming_GTIDEmptyStopTarget guards the fresh-source
// case: an empty executed GTID set is a valid stop target (not "unset"), and
// any streamed set — including an empty one — has already reached it, so the
// stream must stop rather than hang.
func TestShouldContinueStreaming_GTIDEmptyStopTarget(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	s.stopRequested = true
	s.stopAtGTIDSet = mustParseGTID(t, "") // empty executed set on a fresh source

	// Empty streamed set has reached the empty stop target: stop.
	assert.False(t, s.shouldContinueStreaming())

	// A non-empty streamed set also trivially contains the empty target: stop.
	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetTarget)
	assert.False(t, s.shouldContinueStreaming())
}

func TestGetLastStreamedBinlogCoordinate_GTIDMode(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}

	// Nil streamed set yields an empty (zero) GTID coordinate.
	coord := s.GetLastStreamedBinlogCoordinate()
	assert.True(t, coord.IsGTID())
	assert.True(t, coord.IsZero())

	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetTarget)
	coord = s.GetLastStreamedBinlogCoordinate()
	assert.True(t, coord.IsGTID())
	assert.Equal(t, gtidSetTarget, coord.GTIDSet)
}

func TestGetLastStreamedBinlogCoordinate_FilePositionMode(t *testing.T) {
	s := &BinlogStreamer{}
	s.lastStreamedBinlogPosition = mysql.Position{Name: "mysql-bin.000010", Pos: 100}

	coord := s.GetLastStreamedBinlogCoordinate()
	assert.True(t, coord.IsFilePosition())
	assert.Equal(t, "mysql-bin.000010", coord.Position().Name)
	assert.Equal(t, uint32(100), coord.Position().Pos)
}

func TestConnectBinlogStreamerSinceCoordinate_TypeMismatch(t *testing.T) {
	// GTID mode with a file/position coordinate must be rejected before any DB
	// interaction.
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	_, err := s.ConnectBinlogStreamerToMysqlSinceCoordinate(
		NewFilePositionCoordinate(mysql.Position{Name: "mysql-bin.000001", Pos: 4}),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GTID mode requires a GTID coordinate")

	// File/position mode with a GTID coordinate must also be rejected.
	s2 := &BinlogStreamer{}
	_, err = s2.ConnectBinlogStreamerToMysqlSinceCoordinate(NewGTIDCoordinate(gtidSetTarget))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "file/position mode requires a file/position coordinate")
}
