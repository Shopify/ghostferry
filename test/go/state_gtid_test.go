package test

import (
	"encoding/json"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	uuidA        = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	gtidWritten  = uuidA + ":1-100"
	gtidInline   = uuidA + ":1-80"
	gtidTarget   = uuidA + ":1-90"
	gtidExpected = uuidA + ":1-80" // intersection of written and inline
)

func gtidCoord(s string) *ghostferry.BinlogCoordinate {
	c := ghostferry.NewGTIDCoordinate(s)
	return &c
}

func TestSerializableState_GTIDRoundTrip(t *testing.T) {
	state := &ghostferry.SerializableState{
		GhostferryVersion:                           "test-version",
		LastSuccessfulPaginationKeys:                map[string]ghostferry.PaginationKey{},
		CompletedTables:                             map[string]bool{},
		BinlogCoordinateMode:                        ghostferry.BinlogCoordinateGTID,
		LastWrittenBinlogCoordinate:                 gtidCoord(gtidWritten),
		LastStoredBinlogCoordinateForInlineVerifier: gtidCoord(gtidInline),
		LastStoredBinlogCoordinateForTargetVerifier: gtidCoord(gtidTarget),
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)

	var decoded ghostferry.SerializableState
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, ghostferry.BinlogCoordinateGTID, decoded.BinlogCoordinateMode)
	require.NotNil(t, decoded.LastWrittenBinlogCoordinate)
	assert.True(t, decoded.LastWrittenBinlogCoordinate.IsGTID())
	assert.Equal(t, gtidWritten, decoded.LastWrittenBinlogCoordinate.GTIDSet)
	assert.Equal(t, gtidInline, decoded.LastStoredBinlogCoordinateForInlineVerifier.GTIDSet)
	assert.Equal(t, gtidTarget, decoded.LastStoredBinlogCoordinateForTargetVerifier.GTIDSet)
}

// TestSerializableState_FilePositionOmitsGTIDFields guards backward
// compatibility: a file/position state must not emit GTID fields or a mode.
func TestSerializableState_FilePositionOmitsGTIDFields(t *testing.T) {
	state := &ghostferry.SerializableState{
		GhostferryVersion:            "test-version",
		LastSuccessfulPaginationKeys: map[string]ghostferry.PaginationKey{},
		CompletedTables:              map[string]bool{},
		LastWrittenBinlogPosition:    mysql.Position{Name: "mysql-bin.000001", Pos: 4},
	}

	data, err := json.Marshal(state)
	require.NoError(t, err)

	str := string(data)
	assert.NotContains(t, str, "BinlogCoordinateMode")
	assert.NotContains(t, str, "LastWrittenBinlogCoordinate")
	assert.NotContains(t, str, "GTIDSet")
}

func TestSerializableState_MinSourceBinlogCoordinate_GTIDIntersection(t *testing.T) {
	state := &ghostferry.SerializableState{
		BinlogCoordinateMode:                        ghostferry.BinlogCoordinateGTID,
		LastWrittenBinlogCoordinate:                 gtidCoord(gtidWritten),
		LastStoredBinlogCoordinateForInlineVerifier: gtidCoord(gtidInline),
	}

	coord := state.MinSourceBinlogCoordinate()
	assert.True(t, coord.IsGTID())

	// The safe resume point is the intersection: the smaller of the two here.
	got, err := coord.ParsedGTIDSet()
	require.NoError(t, err)
	want, err := mysql.ParseMysqlGTIDSet(gtidExpected)
	require.NoError(t, err)
	assert.True(t, got.Equal(want), "expected intersection %s, got %s", want.String(), got.String())
}

func TestSerializableState_MinSourceBinlogCoordinate_GTIDSingleSide(t *testing.T) {
	state := &ghostferry.SerializableState{
		BinlogCoordinateMode:        ghostferry.BinlogCoordinateGTID,
		LastWrittenBinlogCoordinate: gtidCoord(gtidWritten),
	}

	coord := state.MinSourceBinlogCoordinate()
	assert.True(t, coord.IsGTID())
	assert.Equal(t, gtidWritten, coord.GTIDSet)
}

func TestStateTracker_GTIDUpdateAndSerialize(t *testing.T) {
	st := ghostferry.NewStateTracker(0)

	st.UpdateLastResumableSourceBinlogCoordinate(ghostferry.NewGTIDCoordinate(gtidWritten))
	st.UpdateLastResumableSourceBinlogCoordinateForInlineVerifier(ghostferry.NewGTIDCoordinate(gtidInline))
	st.UpdateLastResumableBinlogCoordinateForTargetVerifier(ghostferry.NewGTIDCoordinate(gtidTarget))

	assert.Equal(t, gtidWritten, st.LastResumableSourceBinlogCoordinate().GTIDSet)
	assert.Equal(t, gtidInline, st.LastResumableSourceBinlogCoordinateForInlineVerifier().GTIDSet)
	assert.Equal(t, gtidTarget, st.LastResumableBinlogCoordinateForTargetVerifier().GTIDSet)

	state := st.Serialize(nil, nil)
	assert.Equal(t, ghostferry.BinlogCoordinateGTID, state.BinlogCoordinateMode)
	require.NotNil(t, state.LastWrittenBinlogCoordinate)
	assert.Equal(t, gtidWritten, state.LastWrittenBinlogCoordinate.GTIDSet)

	// Round-trip through a new tracker resumed from the serialized state.
	resumed := ghostferry.NewStateTrackerFromSerializedState(0, state)
	assert.Equal(t, gtidWritten, resumed.LastResumableSourceBinlogCoordinate().GTIDSet)
	assert.Equal(t, gtidTarget, resumed.LastResumableBinlogCoordinateForTargetVerifier().GTIDSet)
}

func TestStateTracker_FilePositionUpdateStaysFilePosition(t *testing.T) {
	st := ghostferry.NewStateTracker(0)

	st.UpdateLastResumableSourceBinlogCoordinate(
		ghostferry.NewFilePositionCoordinate(mysql.Position{Name: "mysql-bin.000009", Pos: 42}),
	)

	coord := st.LastResumableSourceBinlogCoordinate()
	assert.True(t, coord.IsFilePosition())
	assert.Equal(t, "mysql-bin.000009", coord.Position().Name)

	state := st.Serialize(nil, nil)
	assert.Equal(t, ghostferry.BinlogCoordinateType(""), state.BinlogCoordinateMode)
	assert.Nil(t, state.LastWrittenBinlogCoordinate)
}
