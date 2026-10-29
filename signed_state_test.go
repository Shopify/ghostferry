package ghostferry

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/require"
)

func TestSignedCheckpointStateRoundTrip(t *testing.T) {
	column := schema.TableColumn{Name: "id", Type: schema.TYPE_NUMBER}
	table := &TableSchema{Table: &schema.Table{Schema: "db", Name: "rows", Columns: []schema.TableColumn{column}}, PaginationKeyColumn: &column}
	tracker := NewStateTracker(0)
	require.Nil(t, tracker.LastSuccessfulPaginationKey(table.String(), table))

	for _, value := range []int64{math.MinInt64, -1, 0, 1, math.MaxInt64} {
		tracker.UpdateLastSuccessfulPaginationKey(table.String(), NewInt64KeyWithColumn("id", value), RowStats{NumRows: 1})
		encoded, err := json.Marshal(tracker.Serialize(nil, nil))
		require.NoError(t, err)
		var state SerializableState
		require.NoError(t, json.Unmarshal(encoded, &state))
		tracker = NewStateTrackerFromSerializedState(0, &state)
		require.Equal(t, NewInt64KeyWithColumn("id", value), tracker.LastSuccessfulPaginationKey(table.String(), table))
	}

	// Real maximum checkpoints do not implicitly mark a table completed.
	require.False(t, tracker.IsTableComplete(table.String()))
	tracker.MarkTableAsCompleted(table.String())
	require.True(t, tracker.IsTableComplete(table.String()))
	require.Equal(t, MaxPaginationKey(&column), tracker.LastSuccessfulPaginationKey(table.String(), table))
}

func TestOldUnsignedCheckpointOnSignedColumn(t *testing.T) {
	for _, value := range []uint64{0, 1, math.MaxInt64} {
		state := &SerializableState{
			LastSuccessfulPaginationKeys: map[string]PaginationKey{"db.rows": NewUint64KeyWithColumn("id", value)},
			CompletedTables:              map[string]bool{},
		}
		encoded, err := json.Marshal(state)
		require.NoError(t, err)
		var decoded SerializableState
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		checkpoint := decoded.LastSuccessfulPaginationKeys["db.rows"]
		require.IsType(t, Uint64Key{}, checkpoint)
		require.Zero(t, checkpoint.Compare(NewInt64Key(int64(value))))
		require.Zero(t, NewInt64Key(int64(value)).Compare(checkpoint))
	}
}

func TestNegativeCheckpointsDoNotProduceNegativeSpeed(t *testing.T) {
	tracker := NewStateTracker(4)
	tracker.UpdateLastSuccessfulPaginationKey("db.rows", NewInt64Key(-100), RowStats{})
	require.Zero(t, tracker.iterationSpeedLog.Value.(PaginationKeyPositionLog).Position)
	require.Zero(t, tracker.EstimatedPaginationKeysPerSecond())
	tracker.UpdateLastSuccessfulPaginationKey("db.rows", NewInt64Key(-50), RowStats{})
	require.Equal(t, float64(50), tracker.iterationSpeedLog.Value.(PaginationKeyPositionLog).Position)
	tracker.UpdateLastSuccessfulPaginationKey("db.rows", NewInt64Key(0), RowStats{})
	require.Equal(t, float64(100), tracker.iterationSpeedLog.Value.(PaginationKeyPositionLog).Position)
	rate := tracker.EstimatedPaginationKeysPerSecond()
	require.GreaterOrEqual(t, rate, float64(0))
	require.False(t, math.IsNaN(rate) || math.IsInf(rate, 0))
}

func TestSignedPaginationETA(t *testing.T) {
	for _, tc := range []struct {
		name               string
		target, checkpoint PaginationKey
		rate, want         float64
		completed          bool
	}{
		{"negative", NewInt64Key(-1), NewInt64Key(-101), 10, 10, false},
		{"cross_zero", NewInt64Key(50), NewInt64Key(-50), 10, 10, false},
		{"unknown_negative_start", NewInt64Key(-1), nil, 10, 0, false},
		{"unsigned_start", NewUint64Key(100), nil, 10, 10, false},
		{"legacy_unsigned_checkpoint", NewInt64Key(100), NewUint64Key(50), 10, 5, false},
		{"passed_upper_bound", NewInt64Key(-10), NewInt64Key(-1), 10, 0, false},
		{"completed", NewInt64Key(100), NewInt64Key(50), 10, 0, true},
		{"no_rate", NewInt64Key(100), NewInt64Key(-1), 0, 0, false},
		{"nan_rate", NewInt64Key(100), NewInt64Key(-1), math.NaN(), 0, false},
		{"infinite_rate", NewInt64Key(100), NewInt64Key(-1), math.Inf(1), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &SerializableState{
				LastSuccessfulPaginationKeys: map[string]PaginationKey{},
				CompletedTables:              map[string]bool{"db.rows": tc.completed},
			}
			if tc.checkpoint != nil {
				state.LastSuccessfulPaginationKeys["db.rows"] = tc.checkpoint
			}
			got := estimatePaginationETA(map[string]PaginationKey{"db.rows": tc.target}, state, tc.rate)
			require.Equal(t, tc.want, got)
		})
	}
}
