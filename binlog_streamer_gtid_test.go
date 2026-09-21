package ghostferry

import (
	"sync"
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

func TestRunStopsAtGTIDTransactionBoundary(t *testing.T) {
	target := mustParseGTID(t, gtidSetTarget)
	gtid := func() *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
			Event:  &replication.GTIDEvent{},
		}
	}
	query := func(statement string) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
			Event:  &replication.QueryEvent{Query: []byte(statement), GSet: target},
		}
	}
	rows := func(id int) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2},
			Event:  &replication.RowsEvent{Rows: [][]interface{}{{id}}},
		}
	}
	xid := func() *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.XID_EVENT},
			Event:  &replication.XIDEvent{GSet: target},
		}
	}

	for _, tc := range []struct {
		name           string
		events         []*replication.BinlogEvent
		customHandlers []replication.EventType
		wantRows       [][]interface{}
	}{
		{
			name: "savepoints do not commit",
			events: []*replication.BinlogEvent{
				gtid(), query("BEGIN"), rows(1), query("SAVEPOINT `s`"),
				query("ROLLBACK TO SAVEPOINT `s`"), query("RELEASE SAVEPOINT `s`"), rows(2), xid(),
			},
			wantRows: [][]interface{}{{1}, {2}},
		},
		{
			name:   "empty transaction commits without XID",
			events: []*replication.BinlogEvent{gtid(), query("BEGIN"), query("COMMIT")},
		},
		{
			name:   "rollback closes transaction without XID",
			events: []*replication.BinlogEvent{gtid(), query("BEGIN"), query("ROLLBACK")},
		},
		{
			name:   "standalone DDL commits without XID",
			events: []*replication.BinlogEvent{gtid(), query("CREATE TABLE t (id int)")},
		},
		{
			name:           "custom query handler preserves DDL commit",
			events:         []*replication.BinlogEvent{gtid(), query("CREATE TABLE t (id int)")},
			customHandlers: []replication.EventType{replication.QUERY_EVENT},
		},
		{
			name:           "custom transaction handlers preserve commit",
			events:         []*replication.BinlogEvent{gtid(), query("BEGIN"), rows(1), xid()},
			customHandlers: []replication.EventType{replication.GTID_EVENT, replication.QUERY_EVENT, replication.XID_EVENT},
			wantRows:       [][]interface{}{{1}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := replication.NewBinlogStreamer()
			s := &BinlogStreamer{
				BinlogCoordinateMode: BinlogCoordinateGTID,
				binlogStreamer:       stream,
				binlogSyncer:         replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: 1}),
				stopRequested:        true,
			}
			s.seedGTIDSets(mustParseGTID(t, gtidSetLower))
			s.setStopGTIDSet(target)

			var deliveredRows [][]interface{}
			require.NoError(t, s.AddBinlogEventHandler(replication.WRITE_ROWS_EVENTv2, func(ev *replication.BinlogEvent, query []byte, es *BinlogEventState) ([]byte, error) {
				deliveredRows = append(deliveredRows, ev.Event.(*replication.RowsEvent).Rows...)
				return query, nil
			}))
			for _, eventType := range tc.customHandlers {
				require.NoError(t, s.AddBinlogEventHandler(eventType, func(ev *replication.BinlogEvent, query []byte, es *BinlogEventState) ([]byte, error) {
					return query, nil
				}))
			}
			// Fail deterministically instead of polling forever if the commit is missed.
			require.NoError(t, s.AddBinlogEventHandler(replication.HEARTBEAT_EVENT, func(ev *replication.BinlogEvent, query []byte, es *BinlogEventState) ([]byte, error) {
				t.Fatal("stream continued past the transaction's stop GTID")
				return nil, nil
			}))
			for i, ev := range tc.events {
				ev.Header.LogPos = uint32(100 + i)
				require.NoError(t, stream.AddEventToStreamer(ev))
			}
			require.NoError(t, stream.AddEventToStreamer(&replication.BinlogEvent{
				Header: &replication.EventHeader{EventType: replication.HEARTBEAT_EVENT},
				Event:  &replication.GenericEvent{},
			}))

			s.Run()

			assert.Equal(t, tc.wantRows, deliveredRows)
			reached, err := s.GetLastStreamedBinlogCoordinate().HasReached(NewGTIDCoordinate(gtidSetTarget))
			require.NoError(t, err)
			assert.True(t, reached)
		})
	}
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

// TestGTIDCoordinateAccessorsAreRaceFree reproduces the cross-goroutine hazard
// that Ferry.Progress() creates: one goroutine advances the streamed/stop GTID
// sets (as the streaming loop does on every transaction), while another reads
// them through the coordinate accessors (as Progress does). The underlying
// mysql.GTIDSet is a map, so unsynchronised String()/Clone() against a
// concurrent write is a data race (and a potential fatal map panic). Run with
// -race; it must stay clean.
func TestGTIDCoordinateAccessorsAreRaceFree(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	s.logger = LogWithField("tag", "test")
	s.seedGTIDSets(mustParseGTID(t, gtidSetLower))

	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(3)

	// Writer 1: advances the streamed set at commit boundaries (XIDEvent path).
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			s.setResumableToStreamed()
			s.setLastStreamedGTIDSet(mustParseGTID(t, gtidSetTarget))
		}
	}()

	// Writer 2: records the stop target (FlushAndStop path).
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			s.setStopGTIDSet(mustParseGTID(t, gtidSetPast))
		}
	}()

	// Reader: mirrors Ferry.Progress() reading both coordinates.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = s.GetLastStreamedBinlogCoordinate()
			_ = s.GetStopBinlogCoordinate()
			_ = s.resumableGTIDClone()
		}
	}()

	wg.Wait()
}
