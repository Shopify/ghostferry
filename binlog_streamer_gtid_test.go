package ghostferry

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/google/uuid"
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
	sid := uuid.MustParse("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	gtid := func() *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
			Event:  &replication.GTIDEvent{SID: sid[:], GNO: 100},
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
		wantXAPrepare  bool
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
		{
			name: "XA one phase consumes rows and prepare marker",
			events: []*replication.BinlogEvent{
				gtid(), query("XA START X'01',X'',1"), rows(1), query("XA END X'01',X'',1"),
				{Header: &replication.EventHeader{EventType: replication.XA_PREPARE_LOG_EVENT}, Event: &replication.GenericEvent{Data: []byte{1}}},
			},
			customHandlers: []replication.EventType{replication.GTID_EVENT, replication.QUERY_EVENT},
			wantRows:       [][]interface{}{{1}},
			wantXAPrepare:  true,
		},
		{
			name: "XA opener accepts whitespace and case variation",
			events: []*replication.BinlogEvent{
				gtid(), query("xa\t  BeGiN X'01',X'',1"), rows(1), query("XA END X'01',X'',1"),
				{Header: &replication.EventHeader{EventType: replication.XA_PREPARE_LOG_EVENT}, Event: &replication.GenericEvent{Data: []byte{1}}},
			},
			wantRows:      [][]interface{}{{1}},
			wantXAPrepare: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := replication.NewBinlogStreamer()
			s := &BinlogStreamer{
				BinlogCoordinateMode: BinlogCoordinateGTID,
				binlogStreamer:       stream,
				binlogSyncer:         replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: 1}),
			}
			s.stopRequested.Store(true)
			s.seedGTIDSets(mustParseGTID(t, gtidSetLower))
			s.setStopGTIDSet(target)

			var deliveredRows [][]interface{}
			sawXAPrepare := false
			require.NoError(t, s.AddBinlogEventHandler(replication.XA_PREPARE_LOG_EVENT, func(_ *replication.BinlogEvent, query []byte, _ *BinlogEventState) ([]byte, error) {
				sawXAPrepare = true
				return query, nil
			}))
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
			assert.Equal(t, tc.wantXAPrepare, sawXAPrepare)
			reached, err := s.GetLastStreamedBinlogCoordinate().HasReached(NewGTIDCoordinate(gtidSetTarget))
			require.NoError(t, err)
			assert.True(t, reached)
		})
	}
}

// Failover validation must include in-flight GTIDs even when custom handlers replace the default transaction handlers.
func TestRunIncludesInFlightGTIDInFailoverRequirements(t *testing.T) {
	const sidText = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	sid := uuid.MustParse(sidText)
	for _, finish := range []string{"XID", "COMMIT", "CREATE TABLE t (id int)"} {
		t.Run(finish, func(t *testing.T) {
			stream := replication.NewBinlogStreamer()
			s := &BinlogStreamer{
				BinlogCoordinateMode: BinlogCoordinateGTID,
				binlogStreamer:       stream,
				binlogSyncer:         replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: 1}),
			}
			s.stopRequested.Store(true)
			s.seedGTIDSets(mustParseGTID(t, sidText+":1-99"))
			s.setStopGTIDSet(mustParseGTID(t, gtidSetTarget))
			for _, kind := range []replication.EventType{replication.GTID_EVENT, replication.QUERY_EVENT, replication.XID_EVENT} {
				require.NoError(t, s.AddBinlogEventHandler(kind, func(_ *replication.BinlogEvent, query []byte, _ *BinlogEventState) ([]byte, error) {
					return query, nil
				}))
			}
			checked := false
			require.NoError(t, s.AddBinlogEventHandler(replication.HEARTBEAT_EVENT, func(_ *replication.BinlogEvent, query []byte, _ *BinlogEventState) ([]byte, error) {
				require.False(t, checked, "stream must stop at the final committed coordinate")
				checked = true
				applied, err := s.appliedGTIDSet()
				require.NoError(t, err)
				require.True(t, applied.Contain(mustParseGTID(t, sidText+":100")), "failover must require the in-flight GTID")
				return query, nil
			}))
			add := func(kind replication.EventType, event replication.Event) {
				require.NoError(t, stream.AddEventToStreamer(&replication.BinlogEvent{
					Header: &replication.EventHeader{EventType: kind, LogPos: 100},
					Event:  event,
				}))
			}
			add(replication.GTID_EVENT, &replication.GTIDEvent{SID: sid[:], GNO: 100})
			if finish != "CREATE TABLE t (id int)" {
				add(replication.QUERY_EVENT, &replication.QueryEvent{Query: []byte("BEGIN"), GSet: mustParseGTID(t, gtidSetTarget)})
				add(replication.QUERY_EVENT, &replication.QueryEvent{Query: []byte("SAVEPOINT s"), GSet: mustParseGTID(t, gtidSetTarget)})
			}
			add(replication.HEARTBEAT_EVENT, &replication.GenericEvent{})
			if finish == "XID" {
				add(replication.XID_EVENT, &replication.XIDEvent{GSet: mustParseGTID(t, gtidSetTarget)})
			} else {
				add(replication.QUERY_EVENT, &replication.QueryEvent{Query: []byte(finish), GSet: mustParseGTID(t, gtidSetTarget)})
			}
			add(replication.HEARTBEAT_EVENT, &replication.GenericEvent{})
			s.Run()
			require.True(t, checked)
			require.Empty(t, s.inFlightGTID, "committed transactions must no longer be in flight")
		})
	}
}

func TestShouldContinueStreaming_FilePosition(t *testing.T) {
	s := &BinlogStreamer{}

	assert.True(t, s.shouldContinueStreaming())

	s.stopRequested.Store(true)
	s.stopAtBinlogPosition = mysql.Position{Name: "mysql-bin.000010", Pos: 100}

	s.lastStreamedBinlogPosition = mysql.Position{Name: "mysql-bin.000010", Pos: 50}
	assert.True(t, s.shouldContinueStreaming())

	s.lastStreamedBinlogPosition = mysql.Position{Name: "mysql-bin.000010", Pos: 100}
	assert.False(t, s.shouldContinueStreaming())
}

func TestShouldContinueStreaming_GTID(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}

	assert.True(t, s.shouldContinueStreaming())

	s.stopRequested.Store(true)
	s.stopAtGTIDSet = mustParseGTID(t, gtidSetTarget)

	assert.True(t, s.shouldContinueStreaming())

	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetLower)
	assert.True(t, s.shouldContinueStreaming())

	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetTarget)
	assert.False(t, s.shouldContinueStreaming())

	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetPast)
	assert.False(t, s.shouldContinueStreaming())
}

// TestShouldContinueStreaming_GTIDEmptyStopTarget: an empty executed GTID set
// is a valid stop target (fresh source), so the stream must stop, not hang.
func TestShouldContinueStreaming_GTIDEmptyStopTarget(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	s.stopRequested.Store(true)
	s.stopAtGTIDSet = mustParseGTID(t, "") // empty executed set on a fresh source

	assert.False(t, s.shouldContinueStreaming())

	s.lastStreamedGTIDSet = mustParseGTID(t, gtidSetTarget)
	assert.False(t, s.shouldContinueStreaming())
}

func TestGetLastStreamedBinlogCoordinate_GTIDMode(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}

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
	// Type mismatch must be rejected before any DB interaction.
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	_, err := s.ConnectBinlogStreamerToMysqlSinceCoordinate(
		NewFilePositionCoordinate(mysql.Position{Name: "mysql-bin.000001", Pos: 4}),
	)
	require.Error(t, err)

	s2 := &BinlogStreamer{}
	_, err = s2.ConnectBinlogStreamerToMysqlSinceCoordinate(NewGTIDCoordinate(gtidSetTarget))
	require.Error(t, err)
}

// TestGTIDCoordinateAccessorsAreRaceFree mirrors the streaming loop and
// Ferry.Progress() concurrently writing/reading the GTID sets (maps); must
// stay clean under -race.
func TestGTIDCoordinateAccessorsAreRaceFree(t *testing.T) {
	s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
	s.logger = LogWithField("tag", "test")
	s.seedGTIDSets(mustParseGTID(t, gtidSetLower))
	sid := uuid.MustParse("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	event := &replication.GTIDEvent{SID: sid[:], GNO: 100}

	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if err := s.setResumableBeforeGTID(event); err != nil {
				t.Error(err)
				return
			}
			s.setLastStreamedGTIDSet(mustParseGTID(t, gtidSetTarget))
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			s.setStopGTIDSet(mustParseGTID(t, gtidSetPast))
			s.stopRequested.Store(true)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = s.GetLastStreamedBinlogCoordinate()
			_ = s.GetStopBinlogCoordinate()
			_ = s.resumableGTIDClone()
			_ = s.shouldContinueStreaming()
		}
	}()

	wg.Wait()
}

func TestRunFinishesReplayedTransactionBeforeCutover(t *testing.T) {
	sid := uuid.MustParse("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	for _, stopAt := range []string{"GTID", "first replayed update"} {
		t.Run(stopAt, func(t *testing.T) {
			stream := replication.NewBinlogStreamer()
			s := &BinlogStreamer{
				BinlogCoordinateMode: BinlogCoordinateGTID,
				binlogStreamer:       stream,
				binlogSyncer:         replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: 1}),
			}
			s.seedGTIDSets(mustParseGTID(t, gtidSetLower))
			s.setStopGTIDSet(mustParseGTID(t, gtidSetTarget))
			gtids, updates, value := 0, 0, 0
			require.NoError(t, s.AddBinlogEventHandler(replication.GTID_EVENT, func(_ *replication.BinlogEvent, query []byte, _ *BinlogEventState) ([]byte, error) {
				gtids++
				if gtids == 2 && stopAt == "GTID" {
					s.stopRequested.Store(true)
				}
				return query, nil
			}))
			require.NoError(t, s.AddBinlogEventHandler(replication.UPDATE_ROWS_EVENTv2, func(ev *replication.BinlogEvent, query []byte, _ *BinlogEventState) ([]byte, error) {
				rows := ev.Event.(*replication.RowsEvent).Rows
				if value == rows[0][0].(int) {
					value = rows[1][0].(int)
				}
				updates++
				if updates == 3 && stopAt == "first replayed update" {
					s.stopRequested.Store(true)
				}
				return query, nil
			}))
			require.NoError(t, s.AddBinlogEventHandler(replication.HEARTBEAT_EVENT, func(_ *replication.BinlogEvent, _ []byte, _ *BinlogEventState) ([]byte, error) {
				t.Fatal("stream continued past the replayed commit")
				return nil, nil
			}))
			add := func(kind replication.EventType, event replication.Event) {
				require.NoError(t, stream.AddEventToStreamer(&replication.BinlogEvent{
					Header: &replication.EventHeader{EventType: kind, LogPos: 100}, Event: event,
				}))
			}
			for range 2 {
				add(replication.GTID_EVENT, &replication.GTIDEvent{SID: sid[:], GNO: 100})
				add(replication.QUERY_EVENT, &replication.QueryEvent{Query: []byte("BEGIN"), GSet: mustParseGTID(t, gtidSetTarget)})
				add(replication.UPDATE_ROWS_EVENTv2, &replication.RowsEvent{Rows: [][]interface{}{{0}, {1}}})
				add(replication.UPDATE_ROWS_EVENTv2, &replication.RowsEvent{Rows: [][]interface{}{{1}, {0}}})
				add(replication.XID_EVENT, &replication.XIDEvent{GSet: mustParseGTID(t, gtidSetTarget)})
			}
			add(replication.HEARTBEAT_EVENT, &replication.GenericEvent{})
			s.Run()
			assert.Equal(t, 4, updates, "cutover must finish replaying the transaction")
			assert.Equal(t, 0, value, "cutover must preserve the committed row value")
		})
	}
}

func gtidReplayTableSchema(database string) TableSchemaCache {
	return TableSchemaCache{
		database + ".t": &TableSchema{Table: &schema.Table{
			Schema: database, Name: "t",
			Columns: []schema.TableColumn{{Name: "id", Type: schema.TYPE_NUMBER}, {Name: "v", Type: schema.TYPE_NUMBER}},
		}},
	}
}

func queueGTIDReplayTransaction(t *testing.T, stream *replication.BinlogStreamer) {
	t.Helper()
	sid := uuid.MustParse("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	target := mustParseGTID(t, gtidSetTarget)
	events := []*replication.BinlogEvent{
		{Header: &replication.EventHeader{EventType: replication.GTID_EVENT}, Event: &replication.GTIDEvent{SID: sid[:], GNO: 100}},
		{Header: &replication.EventHeader{EventType: replication.QUERY_EVENT}, Event: &replication.QueryEvent{Query: []byte("BEGIN"), GSet: target}},
		{Header: &replication.EventHeader{EventType: replication.UPDATE_ROWS_EVENTv2}, Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte("review"), Table: []byte("t")}, Rows: [][]interface{}{{1, 0}, {1, 1}},
		}},
		{Header: &replication.EventHeader{EventType: replication.UPDATE_ROWS_EVENTv2}, Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte("review"), Table: []byte("t")}, Rows: [][]interface{}{{1, 1}, {1, 0}},
		}},
		{Header: &replication.EventHeader{EventType: replication.XID_EVENT}, Event: &replication.XIDEvent{GSet: target}},
	}
	for i, event := range events {
		event.Header.LogPos = uint32(100 + i)
		require.NoError(t, stream.AddEventToStreamer(event))
	}
}

func TestRunResumesInterruptedGTIDReplay(t *testing.T) {
	const start = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-99"
	for _, inline := range []bool{false, true} {
		name := "writer_only"
		if inline {
			name = "writer_and_inline"
		}
		t.Run(name, func(t *testing.T) {
			newStreamer := func(coord BinlogCoordinate, groups int) *BinlogStreamer {
				stream := replication.NewBinlogStreamer()
				s := &BinlogStreamer{
					BinlogCoordinateMode: BinlogCoordinateGTID,
					binlogStreamer:       stream,
					binlogSyncer:         replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: 1}),
					TableSchema:          gtidReplayTableSchema("review"),
				}
				s.seedGTIDSets(mustParseGTID(t, coord.GTIDSet))
				s.setStopGTIDSet(mustParseGTID(t, gtidSetTarget))
				for range groups {
					queueGTIDReplayTransaction(t, stream)
				}
				require.NoError(t, s.AddBinlogEventHandler(replication.HEARTBEAT_EVENT, func(_ *replication.BinlogEvent, _ []byte, _ *BinlogEventState) ([]byte, error) {
					t.Fatal("stream continued past the replayed commit")
					return nil, nil
				}))
				require.NoError(t, stream.AddEventToStreamer(&replication.BinlogEvent{
					Header: &replication.EventHeader{EventType: replication.HEARTBEAT_EVENT}, Event: &replication.GenericEvent{},
				}))
				return s
			}
			tracker := NewStateTracker(0)
			advance := func(coord BinlogCoordinate) {
				tracker.UpdateLastResumableSourceBinlogCoordinate(coord)
				if inline {
					tracker.UpdateLastResumableSourceBinlogCoordinateForInlineVerifier(coord)
				}
			}
			advance(NewGTIDCoordinate(start))
			s := newStreamer(NewGTIDCoordinate(start), 2)
			consumer, updates, savedConsumer := 0, 0, 0
			var saved []byte
			apply := func(ev DMLEvent) {
				if consumer == ev.OldValues()[1].(int) {
					consumer = ev.NewValues()[1].(int)
				}
				advance(ev.ResumableBinlogCoordinate())
				updates++
			}
			s.AddEventListener(func(events []DMLEvent) error {
				for _, ev := range events {
					apply(ev)
					if updates == 3 {
						assert.Equal(t, 1, consumer)
						savedConsumer = consumer
						var err error
						saved, err = json.Marshal(tracker.Serialize(nil, nil))
						require.NoError(t, err)
						s.stopRequested.Store(true)
					}
				}
				return nil
			})
			s.Run()
			assert.Equal(t, 4, updates, "graceful cutover must finish the replay")
			assert.Equal(t, 0, consumer)

			var state SerializableState
			require.NoError(t, json.Unmarshal(saved, &state))
			tracker = NewStateTrackerFromSerializedState(0, &state)
			resume, err := state.MinSourceBinlogCoordinate()
			require.NoError(t, err)
			assert.True(t, mustParseGTID(t, start).Equal(mustParseGTID(t, resume.GTIDSet)), "saved floor: %s", resume.GTIDSet)

			consumer, updates = savedConsumer, 0
			resumed := newStreamer(resume, 1)
			resumed.stopRequested.Store(true)
			resumed.AddEventListener(func(events []DMLEvent) error {
				for _, ev := range events {
					apply(ev)
				}
				return nil
			})
			resumed.Run()
			assert.Equal(t, 2, updates, "resume must deliver the interrupted transaction")
			assert.Equal(t, 0, consumer, "resume must repair the saved intermediate row")
		})
	}
}

func TestGTIDResumeFloorExcludesOnlyCurrentTransaction(t *testing.T) {
	const a = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const b = "8e12fa47-71ca-11e1-9e33-c80aa9429999"
	sid := uuid.MustParse(a)
	for _, tc := range []struct {
		name, committed, want string
	}{
		{"new transaction", a + ":1-99", a + ":1-99"},
		{"interior replay preserves later and unrelated GTIDs", a + ":1-150," + b + ":1-3:8-10", a + ":1-99:101-150," + b + ":1-3:8-10"},
		{"sole transaction", a + ":100", ""},
		{"nil committed progress", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &BinlogStreamer{BinlogCoordinateMode: BinlogCoordinateGTID}
			// Start with an existing floor to ensure nil committed progress clears it.
			s.seedGTIDSets(mustParseGTID(t, gtidSetTarget))
			var committed mysql.GTIDSet
			if tc.committed != "" {
				committed = mustParseGTID(t, tc.committed)
			}
			s.setLastStreamedGTIDSet(committed)
			s.setStopGTIDSet(mustParseGTID(t, gtidSetPast))
			beforeCommitted := s.GetLastStreamedBinlogCoordinate()
			beforeStop := s.GetStopBinlogCoordinate()

			require.NoError(t, s.setResumableBeforeGTID(&replication.GTIDEvent{SID: sid[:], GNO: 100}))
			floor := s.resumableGTIDCoordinate()
			assert.True(t, mustParseGTID(t, tc.want).Equal(mustParseGTID(t, floor.GTIDSet)), "resume floor: %s", floor.GTIDSet)
			if tc.want == "" {
				assert.Empty(t, floor.GTIDSet, "empty floor must not serialize a bare UUID")
			}
			assert.True(t, mustParseGTID(t, beforeCommitted.GTIDSet).Equal(mustParseGTID(t, s.GetLastStreamedBinlogCoordinate().GTIDSet)))
			assert.True(t, mustParseGTID(t, beforeStop.GTIDSet).Equal(mustParseGTID(t, s.GetStopBinlogCoordinate().GTIDSet)))
		})
	}
}

type recordingGTIDErrorHandler struct {
	err error
}

func (h *recordingGTIDErrorHandler) ReportError(_ string, err error) {
	h.err = err
}

func (h *recordingGTIDErrorHandler) Fatal(from string, err error) {
	h.ReportError(from, err)
}

func TestRunRejectsInvalidGTIDBeforeRows(t *testing.T) {
	sid := uuid.MustParse("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	for _, tc := range []struct {
		name  string
		event *replication.GTIDEvent
	}{
		{"malformed SID", &replication.GTIDEvent{SID: []byte{1}, GNO: 100}},
		{"zero sequence", &replication.GTIDEvent{SID: sid[:], GNO: 0}},
		{"overflowing sequence", &replication.GTIDEvent{SID: sid[:], GNO: math.MaxInt64}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := replication.NewBinlogStreamer()
			handler := &recordingGTIDErrorHandler{}
			s := &BinlogStreamer{
				BinlogCoordinateMode: BinlogCoordinateGTID,
				binlogStreamer:       stream,
				binlogSyncer:         replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: 1}),
				TableSchema:          gtidReplayTableSchema("review"),
				ErrorHandler:         handler,
			}
			s.seedGTIDSets(mustParseGTID(t, gtidSetLower))
			beforeResumable := s.resumableGTIDCoordinate()
			beforeCommitted := s.GetLastStreamedBinlogCoordinate()
			rowsDelivered := false
			s.AddEventListener(func(_ []DMLEvent) error {
				rowsDelivered = true
				return nil
			})
			require.NoError(t, s.AddBinlogEventHandler(replication.HEARTBEAT_EVENT, func(_ *replication.BinlogEvent, _ []byte, _ *BinlogEventState) ([]byte, error) {
				t.Fatal("stream continued after invalid GTID")
				return nil, nil
			}))
			for _, event := range []*replication.BinlogEvent{
				{Header: &replication.EventHeader{EventType: replication.GTID_EVENT, LogPos: 100}, Event: tc.event},
				{Header: &replication.EventHeader{EventType: replication.UPDATE_ROWS_EVENTv2, LogPos: 101}, Event: &replication.RowsEvent{
					Table: &replication.TableMapEvent{Schema: []byte("review"), Table: []byte("t")}, Rows: [][]interface{}{{1, 0}, {1, 1}},
				}},
				{Header: &replication.EventHeader{EventType: replication.HEARTBEAT_EVENT}, Event: &replication.GenericEvent{}},
			} {
				require.NoError(t, stream.AddEventToStreamer(event))
			}
			s.Run()
			assert.Error(t, handler.err)
			assert.False(t, rowsDelivered)
			assert.True(t, mustParseGTID(t, beforeResumable.GTIDSet).Equal(mustParseGTID(t, s.resumableGTIDCoordinate().GTIDSet)))
			assert.True(t, mustParseGTID(t, beforeCommitted.GTIDSet).Equal(mustParseGTID(t, s.GetLastStreamedBinlogCoordinate().GTIDSet)))
		})
	}
}

// Syncer whose stream the test supplies, so the real recovery loops run without a MySQL server.
type failoverTestSyncer struct {
	cfg    replication.BinlogSyncerConfig
	start  func(mysql.GTIDSet) (*replication.BinlogStreamer, error)
	closed bool
}

func (f *failoverTestSyncer) StartSync(mysql.Position) (*replication.BinlogStreamer, error) {
	return nil, errors.New("file/position streaming is not used by failover tests")
}

func (f *failoverTestSyncer) StartSyncGTID(set mysql.GTIDSet) (*replication.BinlogStreamer, error) {
	return f.start(set)
}

func (f *failoverTestSyncer) Close() { f.closed = true }

const failoverTestSID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func failoverTestEvent(kind replication.EventType, logPos uint32, event replication.Event) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: kind, LogPos: logPos},
		Event:  event,
	}
}

// None of these proves the new master can carry the run forward.
func startupHeaders() []*replication.BinlogEvent {
	return []*replication.BinlogEvent{
		{
			Header: &replication.EventHeader{EventType: replication.ROTATE_EVENT},
			Event:  &replication.RotateEvent{NextLogName: []byte("mysql-bin.000002"), Position: 4},
		},
		failoverTestEvent(replication.FORMAT_DESCRIPTION_EVENT, 0, &replication.FormatDescriptionEvent{}),
		failoverTestEvent(replication.PREVIOUS_GTIDS_EVENT, 120, &replication.GenericEvent{}),
	}
}

func heartbeatEvent() *replication.BinlogEvent {
	return failoverTestEvent(replication.HEARTBEAT_EVENT, 130, &replication.GenericEvent{})
}

func gtidGroupEvents(t *testing.T, sidText string, gno int64, committed string, rows ...[][]interface{}) []*replication.BinlogEvent {
	t.Helper()
	sid := uuid.MustParse(sidText)
	set := mustParseGTID(t, committed)
	events := []*replication.BinlogEvent{
		failoverTestEvent(replication.GTID_EVENT, 140, &replication.GTIDEvent{SID: sid[:], GNO: gno}),
		failoverTestEvent(replication.QUERY_EVENT, 141, &replication.QueryEvent{Query: []byte("BEGIN"), GSet: set}),
	}
	for i, row := range rows {
		events = append(events, failoverTestEvent(replication.UPDATE_ROWS_EVENTv2, uint32(142+i), &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte("review"), Table: []byte("t")}, Rows: row,
		}))
	}
	return append(events, failoverTestEvent(replication.XID_EVENT, 160, &replication.XIDEvent{GSet: set}))
}

type failoverOptions struct {
	start       string
	unseeded    bool
	maxAttempts int
	retryWait   time.Duration
	serverID    uint32
	candidates  []string
	executed    map[string]string
}

type failoverHarness struct {
	t           *testing.T
	streamer    *BinlogStreamer
	runtime     *SourceRuntime
	reconnector *fakeReconnector
	errorLog    *recordingGTIDErrorHandler
	initialDB   *sql.DB
	initial     *replication.BinlogStreamer

	// Indexed by attempt; the last one repeats. An error simulates a synchronous StartSyncGTID failure.
	scripts []func(*replication.BinlogStreamer) error

	syncers    []*failoverTestSyncer
	resumeSets []string
	actions    []func()
}

// GetEvent selects between channels nondeterministically, so state changes
// must run in the handler of the last queued event.
func (h *failoverHarness) at(fn func()) *replication.BinlogEvent {
	h.actions = append(h.actions, fn)
	return failoverTestEvent(replication.STOP_EVENT, 200, &replication.GenericEvent{})
}

func (h *failoverHarness) disconnect(stream *replication.BinlogStreamer, err error) *replication.BinlogEvent {
	return h.at(func() { stream.AddErrorToStreamer(err) })
}

func (h *failoverHarness) queue(stream *replication.BinlogStreamer, events ...*replication.BinlogEvent) error {
	for _, ev := range events {
		if err := stream.AddEventToStreamer(ev); err != nil {
			return err
		}
	}
	return nil
}

func (h *failoverHarness) newSyncer(cfg replication.BinlogSyncerConfig) binlogSyncer {
	syncer := &failoverTestSyncer{cfg: cfg}
	syncer.start = func(resume mysql.GTIDSet) (*replication.BinlogStreamer, error) {
		call := len(h.resumeSets)
		h.resumeSets = append(h.resumeSets, resume.String())

		script := h.scripts[len(h.scripts)-1]
		if call < len(h.scripts) {
			script = h.scripts[call]
		}

		stream := replication.NewBinlogStreamer()
		if err := script(stream); err != nil {
			return nil, err
		}
		return stream, nil
	}
	h.syncers = append(h.syncers, syncer)
	return syncer
}

func newFailoverHarness(t *testing.T, opts failoverOptions) *failoverHarness {
	t.Helper()

	initialDB := newSourceRuntimeQueryDB(t, []string{"id"}, [][]driver.Value{{int64(1)}})
	deadConfig := runtimeTestConfig("dead")
	runtime := NewSourceRuntime(initialDB, deadConfig)
	t.Cleanup(runtime.CloseRetired)

	candidates := make([]*DatabaseConfig, 0, len(opts.candidates))
	for _, host := range opts.candidates {
		candidates = append(candidates, runtimeTestConfig(host))
	}

	h := &failoverHarness{
		t:           t,
		runtime:     runtime,
		reconnector: &fakeReconnector{runtime: runtime, candidates: candidates, executed: opts.executed},
		errorLog:    &recordingGTIDErrorHandler{},
		initialDB:   initialDB,
		initial:     replication.NewBinlogStreamer(),
	}

	serverID := opts.serverID
	if serverID == 0 {
		serverID = 4242
	}

	s := &BinlogStreamer{
		DB:                   initialDB,
		DBConfig:             deadConfig,
		MyServerId:           serverID,
		BinlogCoordinateMode: BinlogCoordinateGTID,
		TableSchema:          gtidReplayTableSchema("review"),
		ErrorHandler:         h.errorLog,
		SourceReconnector:    h.reconnector,
		MasterFailoverRecovery: &MasterFailoverRecoveryConfig{
			MaxAttempts: opts.maxAttempts,
			RetryWait:   opts.retryWait,
		},
		binlogStreamer:      h.initial,
		binlogSyncerFactory: h.newSyncer,
	}
	s.binlogSyncer = &failoverTestSyncer{start: func(mysql.GTIDSet) (*replication.BinlogStreamer, error) {
		return h.initial, nil
	}}
	if !opts.unseeded {
		s.seedGTIDSets(mustParseGTID(t, opts.start))
	}

	require.NoError(t, s.AddBinlogEventHandler(replication.STOP_EVENT, func(_ *replication.BinlogEvent, query []byte, _ *BinlogEventState) ([]byte, error) {
		if len(h.actions) == 0 {
			return query, errors.New("unexpected control event")
		}
		action := h.actions[0]
		h.actions = h.actions[1:]
		action()
		return query, nil
	}))

	h.streamer = s
	return h
}

func (h *failoverHarness) assertNothingPublished() {
	h.t.Helper()
	assert.Empty(h.t, h.reconnector.published, "no candidate may be published")
	assert.Same(h.t, h.initialDB, h.runtime.DB(), "the source must not change")
	assert.Empty(h.t, h.runtime.retired, "a rejected candidate must not retire the current source")
	for i, db := range h.reconnector.attempted {
		assert.True(h.t, isClosed(h.t, db), "candidate handle %d must be closed", i)
	}
	for i, syncer := range h.syncers {
		assert.True(h.t, syncer.closed, "candidate syncer %d must be closed", i)
	}
	assert.False(h.t, isClosed(h.t, h.initialDB), "the published source must stay open")
}

func TestFailoverRecoveryExhaustsAttemptsWithoutPublishing(t *testing.T) {
	const retryWait = 30 * time.Millisecond
	lost := errors.New("connection to the source was lost")
	refused := errors.New("candidate refused the dump request")

	for _, tc := range []struct {
		name      string
		start     string
		candidate func(*failoverHarness, *replication.BinlogStreamer) error
	}{
		{
			name:  "synchronous start failure",
			start: failoverTestSID + ":1-99",
			candidate: func(_ *failoverHarness, _ *replication.BinlogStreamer) error {
				return refused
			},
		},
		{
			name:  "asynchronous refusal after headers",
			start: failoverTestSID + ":1-99",
			candidate: func(h *failoverHarness, stream *replication.BinlogStreamer) error {
				return h.queue(stream, append(startupHeaders(), h.disconnect(stream, refused))...)
			},
		},
		{
			name:  "partial transaction then failure",
			start: failoverTestSID + ":1-99",
			candidate: func(h *failoverHarness, stream *replication.BinlogStreamer) error {
				partial := gtidGroupEvents(t, failoverTestSID, 100, gtidSetTarget, [][]interface{}{{1, 0}, {1, 1}})
				return h.queue(stream, append(partial[:len(partial)-1], h.disconnect(stream, refused))...)
			},
		},
		{
			name:  "replay of an already committed group then failure",
			start: gtidSetTarget,
			candidate: func(h *failoverHarness, stream *replication.BinlogStreamer) error {
				replay := gtidGroupEvents(t, failoverTestSID, 100, gtidSetTarget, [][]interface{}{{1, 0}, {1, 1}})
				return h.queue(stream, append(replay, h.disconnect(stream, refused))...)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newFailoverHarness(t, failoverOptions{
				start:       tc.start,
				maxAttempts: 3,
				retryWait:   retryWait,
				candidates:  []string{"promoted"},
				executed:    map[string]string{"promoted": gtidSetPast},
			})
			h.scripts = []func(*replication.BinlogStreamer) error{
				func(stream *replication.BinlogStreamer) error { return tc.candidate(h, stream) },
			}
			require.NoError(t, h.queue(h.initial, h.disconnect(h.initial, lost)))

			h.streamer.Run()

			require.Error(t, h.errorLog.err, "exhausted recovery must be fatal")
			assert.Equal(t, 3, h.reconnector.calls, "a failed attempt must not extend the budget")
			h.assertNothingPublished()

			require.Len(t, h.reconnector.callTimes, 3)
			for i := 1; i < len(h.reconnector.callTimes); i++ {
				assert.GreaterOrEqual(t,
					h.reconnector.callTimes[i].Sub(h.reconnector.callTimes[i-1]), retryWait,
					"attempt %d must wait out the retry interval", i+1)
			}
		})
	}
}

func TestFailoverRecoveryPublishesOnlyAfterUsableStream(t *testing.T) {
	const start = failoverTestSID + ":1-99"
	lost := errors.New("connection to the source was lost")
	refused := errors.New("candidate refused the dump request")

	h := newFailoverHarness(t, failoverOptions{
		start:       start,
		maxAttempts: 4,
		retryWait:   time.Millisecond,
		serverID:    7331,
		candidates:  []string{"host-b", "host-c"},
		executed: map[string]string{
			"host-b": gtidSetTarget,
			"host-c": gtidSetTarget,
		},
	})
	h.streamer.setStopGTIDSet(mustParseGTID(t, gtidSetTarget))
	h.streamer.stopRequested.Store(true)

	// host-c's script replays the group host-b never delivered.
	h.scripts = []func(*replication.BinlogStreamer) error{
		func(stream *replication.BinlogStreamer) error {
			return h.queue(stream, append(startupHeaders(), h.disconnect(stream, refused))...)
		},
		func(stream *replication.BinlogStreamer) error {
			replay := gtidGroupEvents(t, failoverTestSID, 100, gtidSetTarget,
				[][]interface{}{{1, 0}, {1, 1}}, [][]interface{}{{1, 1}, {1, 0}})
			return h.queue(stream, append(startupHeaders(), replay...)...)
		},
	}

	// The interrupted transaction delivered its first update before dying, so replay must repair the row.
	value, updates := 0, 0
	h.streamer.AddEventListener(func(events []DMLEvent) error {
		for _, ev := range events {
			if value == ev.OldValues()[1].(int) {
				value = ev.NewValues()[1].(int)
			}
			updates++
		}
		return nil
	})

	interrupted := gtidGroupEvents(t, failoverTestSID, 100, gtidSetTarget, [][]interface{}{{1, 0}, {1, 1}})
	require.NoError(t, h.queue(h.initial,
		append(interrupted[:len(interrupted)-1], h.disconnect(h.initial, lost))...))

	h.streamer.Run()

	require.NoError(t, h.errorLog.err)
	assert.Equal(t, 2, h.reconnector.calls, "the header-only candidate must not be accepted")
	require.Len(t, h.reconnector.published, 1)
	assert.Equal(t, 3, updates, "the replay must redeliver the interrupted transaction")
	assert.Equal(t, 0, value, "the replay must repair the intermediate row value")
	assert.True(t, mustParseGTID(t, gtidSetTarget).Equal(mustParseGTID(t, h.streamer.lastStreamedGTIDString())),
		"the accepted candidate's commit must advance the streamed set")

	require.Len(t, h.reconnector.previousSeen, 2)
	assert.Equal(t, "dead", h.reconnector.previousSeen[0].Host)
	assert.Equal(t, "host-b", h.reconnector.previousSeen[1].Host)

	// The displaced handle is retained until the embedder declares quiescence.
	published := h.reconnector.published[0]
	assert.Same(t, published, h.runtime.DB())
	assert.Same(t, published, h.streamer.currentDB())
	assert.Equal(t, "host-c", h.streamer.currentDBConfig().Host)
	var id int64
	require.NoError(t, h.initialDB.QueryRow("SELECT 1").Scan(&id),
		"work bound to the displaced handle must keep running")

	require.Len(t, h.reconnector.attempted, 2)
	assert.True(t, isClosed(t, h.reconnector.attempted[0]), "the rejected handle must be closed")
	assert.False(t, isClosed(t, published), "the published handle must stay open")

	require.Len(t, h.syncers, 2)
	for i, syncer := range h.syncers {
		assert.Equal(t, uint32(7331), syncer.cfg.ServerID, "syncer %d", i)
		assert.Equal(t, failoverHeartbeatPeriod, syncer.cfg.HeartbeatPeriod, "syncer %d", i)
	}
	assert.True(t, h.syncers[0].closed, "the rejected syncer must be closed before another is built")
	assert.True(t, h.syncers[1].closed, "Run closes the installed syncer when it exits")

	h.runtime.CloseRetired()
	assert.True(t, isClosed(t, h.initialDB), "explicit retirement releases the displaced handle")
}

func TestFailoverRecoveryRetainsRequiredGTIDsAcrossAttempts(t *testing.T) {
	const otherSID = "8e12fa47-71ca-11e1-9e33-c80aa9429999"
	const thirdSID = "9f13fa47-71ca-11e1-9e33-c80aa9429111"
	const start = failoverTestSID + ":1-99"
	stopTarget := otherSID + ":1-7"
	lost := errors.New("connection to the source was lost")
	refused := errors.New("candidate refused the dump request")

	h := newFailoverHarness(t, failoverOptions{
		start:       start,
		maxAttempts: 4,
		retryWait:   time.Millisecond,
		candidates:  []string{"host-b", "host-c", "host-d"},
		executed: map[string]string{
			// host-b and host-c hold the required sets, so they reach the stream trial and fail there.
			"host-b": gtidSetTarget + "," + stopTarget,
			"host-c": gtidSetTarget + "," + stopTarget,
			"host-d": gtidSetTarget + "," + stopTarget + "," + thirdSID + ":1-5",
		},
	})
	h.streamer.setStopGTIDSet(mustParseGTID(t, stopTarget))
	h.streamer.stopRequested.Store(true)

	h.scripts = []func(*replication.BinlogStreamer) error{
		func(stream *replication.BinlogStreamer) error {
			return h.queue(stream, append(startupHeaders(), h.disconnect(stream, refused))...)
		},
		func(stream *replication.BinlogStreamer) error {
			partial := gtidGroupEvents(t, thirdSID, 5, gtidSetTarget+","+thirdSID+":5", [][]interface{}{{1, 0}, {1, 1}})
			return h.queue(stream, append(partial[:len(partial)-1], h.disconnect(stream, refused))...)
		},
		func(stream *replication.BinlogStreamer) error {
			replay := gtidGroupEvents(t, failoverTestSID, 100,
				gtidSetTarget+","+stopTarget+","+thirdSID+":1-5", [][]interface{}{{1, 1}, {1, 0}})
			return h.queue(stream, replay...)
		},
	}

	// A:100's rows reached listeners before the source died.
	interrupted := gtidGroupEvents(t, failoverTestSID, 100, gtidSetTarget, [][]interface{}{{1, 0}, {1, 1}})
	require.NoError(t, h.queue(h.initial,
		append(interrupted[:len(interrupted)-1], h.disconnect(h.initial, lost))...))

	h.streamer.Run()

	require.NoError(t, h.errorLog.err)
	require.Len(t, h.reconnector.requiredSeen, 3)

	emitted := mustParseGTID(t, failoverTestSID+":100")
	stop := mustParseGTID(t, stopTarget)
	inFlightElsewhere := mustParseGTID(t, thirdSID+":5")
	for i, required := range h.reconnector.requiredSeen {
		require.NotNil(t, required, "attempt %d", i+1)
		assert.True(t, required.Contain(emitted), "attempt %d must still require the emitted GTID", i+1)
		assert.True(t, required.Contain(stop), "attempt %d must still require the stop target", i+1)
		if i >= 2 {
			assert.True(t, required.Contain(inFlightElsewhere),
				"attempt %d must require the GTID a failed trial emitted", i+1)
		}
	}

	// A containment-rejected candidate never starts a syncer, so 3 syncers prove every script ran.
	require.Len(t, h.syncers, 3)
	require.Len(t, h.resumeSets, 3)

	// Reseeding the replay floor per stream must not weaken the requirement.
	for i, resume := range h.resumeSets {
		assert.False(t, mustParseGTID(t, resume).Contain(emitted),
			"attempt %d resumes before the interrupted group", i+1)
	}
}

func TestFailoverRecoveryHeartbeatAcceptance(t *testing.T) {
	lost := errors.New("connection to the source was lost")
	refused := errors.New("candidate refused the dump request")

	t.Run("idle heartbeat accepts an empty source", func(t *testing.T) {
		h := newFailoverHarness(t, failoverOptions{
			start:       "",
			maxAttempts: 2,
			retryWait:   time.Millisecond,
			candidates:  []string{"promoted"},
			executed:    map[string]string{"promoted": ""},
		})
		h.scripts = []func(*replication.BinlogStreamer) error{
			func(stream *replication.BinlogStreamer) error {
				return h.queue(stream, append(startupHeaders(), heartbeatEvent(), h.at(func() {
					// Stop the run once the candidate has been published.
					h.streamer.setStopGTIDSet(mustParseGTID(t, ""))
					h.streamer.stopRequested.Store(true)
				}))...)
			},
		}
		require.NoError(t, h.queue(h.initial, h.disconnect(h.initial, lost)))

		h.streamer.Run()

		require.NoError(t, h.errorLog.err)
		assert.Equal(t, 1, h.reconnector.calls)
		require.Len(t, h.reconnector.published, 1)
		assert.Same(t, h.reconnector.published[0], h.runtime.DB())
	})

	t.Run("heartbeat inside an interrupted group is not progress", func(t *testing.T) {
		const start = failoverTestSID + ":1-99"
		h := newFailoverHarness(t, failoverOptions{
			start:       start,
			maxAttempts: 3,
			retryWait:   time.Millisecond,
			candidates:  []string{"host-b", "host-c"},
			executed:    map[string]string{"host-b": gtidSetTarget, "host-c": gtidSetTarget},
		})
		h.streamer.setStopGTIDSet(mustParseGTID(t, gtidSetTarget))
		h.streamer.stopRequested.Store(true)

		h.scripts = []func(*replication.BinlogStreamer) error{
			func(stream *replication.BinlogStreamer) error {
				partial := gtidGroupEvents(t, failoverTestSID, 100, gtidSetTarget, [][]interface{}{{1, 0}, {1, 1}})
				events := append(partial[:len(partial)-1], heartbeatEvent())
				return h.queue(stream, append(events, h.disconnect(stream, refused))...)
			},
			func(stream *replication.BinlogStreamer) error {
				return h.queue(stream, gtidGroupEvents(t, failoverTestSID, 100, gtidSetTarget,
					[][]interface{}{{1, 0}, {1, 1}})...)
			},
		}
		require.NoError(t, h.queue(h.initial, h.disconnect(h.initial, lost)))

		h.streamer.Run()

		require.NoError(t, h.errorLog.err)
		assert.Equal(t, 2, h.reconnector.calls, "an in-group heartbeat must not accept a candidate")
		require.Len(t, h.reconnector.published, 1)
		assert.Equal(t, "host-c", h.streamer.currentDBConfig().Host)
	})
}

func TestFailoverRecoveryRevalidatesStopTargetRecordedDuringTrial(t *testing.T) {
	const start = failoverTestSID + ":1-99"
	const otherSID = "8e12fa47-71ca-11e1-9e33-c80aa9429999"
	lost := errors.New("connection to the source was lost")
	lateStop := otherSID + ":1-9"

	h := newFailoverHarness(t, failoverOptions{
		start:       start,
		maxAttempts: 1,
		retryWait:   time.Millisecond,
		candidates:  []string{"promoted"},
		// The candidate holds the emitted transaction but not the stop target cutover records during the trial.
		executed: map[string]string{"promoted": gtidSetTarget},
	})

	h.scripts = []func(*replication.BinlogStreamer) error{
		func(stream *replication.BinlogStreamer) error {
			replay := gtidGroupEvents(t, failoverTestSID, 100, gtidSetTarget, [][]interface{}{{1, 0}, {1, 1}})
			events := append([]*replication.BinlogEvent{h.at(func() {
				h.streamer.setStopGTIDSet(mustParseGTID(t, lateStop))
				h.streamer.stopRequested.Store(true)
			})}, replay...)
			return h.queue(stream, events...)
		},
	}
	require.NoError(t, h.queue(h.initial, h.disconnect(h.initial, lost)))

	h.streamer.Run()

	require.Error(t, h.errorLog.err, "a candidate missing the new stop target must not be published")
	require.Len(t, h.reconnector.finalRequired, 1)
	assert.True(t, h.reconnector.finalRequired[0].Contain(mustParseGTID(t, lateStop)),
		"the stop target recorded during the trial must join the requirement")
	h.assertNothingPublished()
}

func TestFailoverRecoveryAbortsOnConsumerError(t *testing.T) {
	const start = failoverTestSID + ":1-99"
	lost := errors.New("connection to the source was lost")
	consumerErr := errors.New("listener rejected the event")

	h := newFailoverHarness(t, failoverOptions{
		start:       start,
		maxAttempts: 5,
		retryWait:   time.Millisecond,
		candidates:  []string{"promoted"},
		executed:    map[string]string{"promoted": gtidSetTarget},
	})
	h.streamer.AddEventListener(func(_ []DMLEvent) error { return consumerErr })
	h.scripts = []func(*replication.BinlogStreamer) error{
		func(stream *replication.BinlogStreamer) error {
			return h.queue(stream, gtidGroupEvents(t, failoverTestSID, 100, gtidSetTarget,
				[][]interface{}{{1, 0}, {1, 1}})...)
		},
	}
	require.NoError(t, h.queue(h.initial, h.disconnect(h.initial, lost)))

	h.streamer.Run()

	require.Error(t, h.errorLog.err)
	assert.ErrorIs(t, h.errorLog.err, consumerErr)
	assert.Equal(t, 1, h.reconnector.calls, "a consumer error must not spend the retry budget")
	h.assertNothingPublished()
}

func TestFailoverRecoveryRequiresInitializedGTIDState(t *testing.T) {
	lost := errors.New("connection to the source was lost")

	t.Run("uninitialized sets fail before resolving", func(t *testing.T) {
		h := newFailoverHarness(t, failoverOptions{
			unseeded:    true,
			maxAttempts: 3,
			retryWait:   time.Millisecond,
			candidates:  []string{"promoted"},
			executed:    map[string]string{"promoted": gtidSetTarget},
		})
		h.scripts = []func(*replication.BinlogStreamer) error{
			func(*replication.BinlogStreamer) error {
				t.Error("recovery must not contact the resolver without a GTID state")
				return errors.New("unexpected candidate")
			},
		}
		require.NoError(t, h.queue(h.initial, h.disconnect(h.initial, lost)))

		h.streamer.Run()

		require.Error(t, h.errorLog.err)
		assert.Equal(t, 0, h.reconnector.calls)
		h.assertNothingPublished()
	})

	t.Run("uninitialized server id fails before resolving", func(t *testing.T) {
		h := newFailoverHarness(t, failoverOptions{
			start:       failoverTestSID + ":1-99",
			maxAttempts: 3,
			retryWait:   time.Millisecond,
			candidates:  []string{"promoted"},
			executed:    map[string]string{"promoted": gtidSetTarget},
		})
		h.streamer.MyServerId = 0
		h.scripts = []func(*replication.BinlogStreamer) error{
			func(*replication.BinlogStreamer) error {
				t.Error("recovery must not contact the resolver without a server id")
				return errors.New("unexpected candidate")
			},
		}
		require.NoError(t, h.queue(h.initial, h.disconnect(h.initial, lost)))

		h.streamer.Run()

		require.Error(t, h.errorLog.err)
		assert.Equal(t, 0, h.reconnector.calls)
	})

	t.Run("a parsed empty set is a valid state", func(t *testing.T) {
		h := newFailoverHarness(t, failoverOptions{
			start:       "",
			maxAttempts: 1,
			retryWait:   time.Millisecond,
			candidates:  []string{"promoted"},
			executed:    map[string]string{"promoted": ""},
		})
		h.scripts = []func(*replication.BinlogStreamer) error{
			func(stream *replication.BinlogStreamer) error {
				return h.queue(stream, heartbeatEvent(), h.at(func() {
					h.streamer.setStopGTIDSet(mustParseGTID(t, ""))
					h.streamer.stopRequested.Store(true)
				}))
			},
		}
		require.NoError(t, h.queue(h.initial, h.disconnect(h.initial, lost)))

		h.streamer.Run()

		require.NoError(t, h.errorLog.err)
		assert.Equal(t, 1, h.reconnector.calls)
		assert.Len(t, h.reconnector.published, 1)
	})
}
