package ghostferry

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
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

	// Stop requested but no target recorded yet: keep going.
	assert.True(t, s.shouldContinueStreaming())

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
