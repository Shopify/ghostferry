package ghostferry

import (
	"container/ring"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// StateTracker design
// ===================
//
// General Overview
// ----------------
//
// The state tracker keeps track of the progress of Ghostferry so it can be
// interrupted and resumed. The state tracker is supposed to be initialized and
// managed by the Ferry. Each Ghostferry components, such as the `BatchWriter`,
// will get passed an instance of the StateTracker. During the run, the
// components will update their last successful components to the state tracker
// instance given via the state tracker API defined here.
//
// The states stored in the state tracker can be copied into a
// serialization-friendly struct (`SerializableState`), which can then be
// dumped using something like JSON. Assuming the rest of Ghostferry used the
// API of the state tracker correctlym this can be done at any point during the
// Ghostferry run and the resulting state can be resumed from without data
// loss.  The same `SerializableState` is used as an input to `Ferry`, which
// will instruct the `Ferry` to resume a previously interrupted run.

type SerializableState struct {
	GhostferryVersion         string
	LastKnownTableSchemaCache TableSchemaCache

	LastSuccessfulPaginationKeys              map[string]PaginationKey
	CompletedTables                           map[string]bool
	LastWrittenBinlogPosition                 mysql.Position
	BinlogVerifyStore                         BinlogVerifySerializedStore
	LastStoredBinlogPositionForInlineVerifier mysql.Position
	LastStoredBinlogPositionForTargetVerifier mysql.Position

	// BinlogCoordinateMode records which coordinate representation the state
	// below was produced with. Empty means file/position (legacy states).
	BinlogCoordinateMode BinlogCoordinateType `json:",omitempty"`

	// GTID counterparts of the three binlog positions above. They are only
	// populated when BinlogCoordinateMode is "gtid". Pointers + omitempty keep
	// file/position states byte-for-byte compatible with older versions.
	LastWrittenBinlogCoordinate                 *BinlogCoordinate `json:",omitempty"`
	LastStoredBinlogCoordinateForInlineVerifier *BinlogCoordinate `json:",omitempty"`
	LastStoredBinlogCoordinateForTargetVerifier *BinlogCoordinate `json:",omitempty"`
}

func (s *SerializableState) MarshalJSON() ([]byte, error) {
	// Create an alias to avoid infinite recursion, but change the map type
	type Alias SerializableState
	aux := &struct {
		LastSuccessfulPaginationKeys map[string]json.RawMessage
		*Alias
	}{
		Alias:                        (*Alias)(s),
		LastSuccessfulPaginationKeys: make(map[string]json.RawMessage),
	}

	for k, v := range s.LastSuccessfulPaginationKeys {
		b, err := v.MarshalJSON()
		if err != nil {
			return nil, err
		}
		aux.LastSuccessfulPaginationKeys[k] = b
	}

	return json.Marshal(aux)
}

func (s *SerializableState) UnmarshalJSON(data []byte) error {
	type Alias SerializableState
	aux := &struct {
		LastSuccessfulPaginationKeys map[string]json.RawMessage
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	s.LastSuccessfulPaginationKeys = make(map[string]PaginationKey)
	for k, v := range aux.LastSuccessfulPaginationKeys {
		pk, err := UnmarshalPaginationKey(v)
		if err != nil {
			return err
		}
		s.LastSuccessfulPaginationKeys[k] = pk
	}

	return nil
}

func (s *SerializableState) MinSourceBinlogPosition() mysql.Position {
	nilPosition := mysql.Position{}
	if s.LastWrittenBinlogPosition == nilPosition {
		return s.LastStoredBinlogPositionForInlineVerifier
	}

	if s.LastStoredBinlogPositionForInlineVerifier == nilPosition {
		return s.LastWrittenBinlogPosition
	}

	if s.LastWrittenBinlogPosition.Compare(s.LastStoredBinlogPositionForInlineVerifier) >= 0 {
		return s.LastStoredBinlogPositionForInlineVerifier
	} else {
		return s.LastWrittenBinlogPosition
	}
}

// coordinateMode returns the effective coordinate mode for this serialized
// state, treating the empty value as file/position for legacy states.
func (s *SerializableState) coordinateMode() BinlogCoordinateType {
	if s.BinlogCoordinateMode == "" {
		return BinlogCoordinateFilePosition
	}
	return s.BinlogCoordinateMode
}

// WrittenSourceBinlogCoordinate returns the last written source coordinate as a
// BinlogCoordinate, matching the state's coordinate mode.
func (s *SerializableState) WrittenSourceBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID && s.LastWrittenBinlogCoordinate != nil {
		return *s.LastWrittenBinlogCoordinate
	}
	return NewFilePositionCoordinate(s.LastWrittenBinlogPosition)
}

// InlineVerifierSourceBinlogCoordinate returns the inline verifier's stored
// source coordinate as a BinlogCoordinate, matching the state's coordinate mode.
func (s *SerializableState) InlineVerifierSourceBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID && s.LastStoredBinlogCoordinateForInlineVerifier != nil {
		return *s.LastStoredBinlogCoordinateForInlineVerifier
	}
	return NewFilePositionCoordinate(s.LastStoredBinlogPositionForInlineVerifier)
}

// TargetVerifierBinlogCoordinate returns the target verifier's stored
// coordinate as a BinlogCoordinate, matching the state's coordinate mode.
func (s *SerializableState) TargetVerifierBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID && s.LastStoredBinlogCoordinateForTargetVerifier != nil {
		return *s.LastStoredBinlogCoordinateForTargetVerifier
	}
	return NewFilePositionCoordinate(s.LastStoredBinlogPositionForTargetVerifier)
}

// MinSourceBinlogCoordinate returns the safe source resume coordinate.
//
// For file/position mode this is the coordinate-typed counterpart of
// MinSourceBinlogPosition (the earlier of the writer and inline-verifier
// positions). For GTID mode the safe resume point is the intersection of the
// writer and inline-verifier GTID sets, since resuming must not skip events
// either consumer had not yet durably processed. When only one side is present,
// that side is used.
func (s *SerializableState) MinSourceBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() != BinlogCoordinateGTID {
		return NewFilePositionCoordinate(s.MinSourceBinlogPosition())
	}

	written := s.LastWrittenBinlogCoordinate
	inline := s.LastStoredBinlogCoordinateForInlineVerifier

	if written == nil && inline == nil {
		return NewGTIDCoordinate("")
	}
	if written == nil {
		return *inline
	}
	if inline == nil {
		return *written
	}

	// Safe resume is the intersection: only GTIDs that both the writer and the
	// inline verifier have durably processed can be skipped on resume.
	writtenSet, err := written.ParsedGTIDSet()
	if err != nil {
		return *written
	}
	inlineSet, err := inline.ParsedGTIDSet()
	if err != nil {
		return *inline
	}

	intersection := intersectGTIDSets(writtenSet, inlineSet)
	return NewGTIDCoordinate(intersection.String())
}

// For tracking the speed of the copy
type PaginationKeyPositionLog struct {
	Position float64
	At       time.Time
}

func newSpeedLogRing(speedLogCount int) *ring.Ring {
	if speedLogCount <= 0 {
		return nil
	}

	speedLog := ring.New(speedLogCount)
	speedLog.Value = PaginationKeyPositionLog{
		Position: 0,
		At:       time.Now(),
	}

	return speedLog
}

type RowStats struct {
	NumRows  uint64
	NumBytes uint64
}

type StateTracker struct {
	BinlogRWMutex *sync.RWMutex
	CopyRWMutex   *sync.RWMutex

	lastWrittenBinlogPosition                 mysql.Position
	lastStoredBinlogPositionForInlineVerifier mysql.Position
	lastStoredBinlogPositionForTargetVerifier mysql.Position

	// GTID coordinates, only populated when the tracker operates in GTID mode.
	// They are stored alongside (not instead of) the file/position fields so
	// that switching the read path is a mode decision, not a data migration.
	lastWrittenBinlogCoordinate                 *BinlogCoordinate
	lastStoredBinlogCoordinateForInlineVerifier *BinlogCoordinate
	lastStoredBinlogCoordinateForTargetVerifier *BinlogCoordinate

	lastSuccessfulPaginationKeys map[string]PaginationKey
	completedTables              map[string]bool

	// TODO: Performance tracking should be refactored out of the state tracker,
	// as it confuses the focus of this struct.
	iterationSpeedLog       *ring.Ring
	rowStatsWrittenPerTable map[string]RowStats
}

func NewStateTracker(speedLogCount int) *StateTracker {
	return &StateTracker{
		BinlogRWMutex: &sync.RWMutex{},
		CopyRWMutex:   &sync.RWMutex{},

		lastSuccessfulPaginationKeys: make(map[string]PaginationKey),
		completedTables:              make(map[string]bool),
		iterationSpeedLog:            newSpeedLogRing(speedLogCount),
		rowStatsWrittenPerTable:      make(map[string]RowStats),
	}
}

// serializedState is a state the tracker should start from, as opposed to
// starting from the beginning.
func NewStateTrackerFromSerializedState(speedLogCount int, serializedState *SerializableState) *StateTracker {
	s := NewStateTracker(speedLogCount)
	s.lastSuccessfulPaginationKeys = serializedState.LastSuccessfulPaginationKeys
	s.completedTables = serializedState.CompletedTables
	s.lastWrittenBinlogPosition = serializedState.LastWrittenBinlogPosition
	s.lastStoredBinlogPositionForInlineVerifier = serializedState.LastStoredBinlogPositionForInlineVerifier
	s.lastStoredBinlogPositionForTargetVerifier = serializedState.LastStoredBinlogPositionForTargetVerifier
	s.lastWrittenBinlogCoordinate = serializedState.LastWrittenBinlogCoordinate
	s.lastStoredBinlogCoordinateForInlineVerifier = serializedState.LastStoredBinlogCoordinateForInlineVerifier
	s.lastStoredBinlogCoordinateForTargetVerifier = serializedState.LastStoredBinlogCoordinateForTargetVerifier
	return s
}

func (s *StateTracker) UpdateLastResumableSourceBinlogPosition(pos mysql.Position) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	s.lastWrittenBinlogPosition = pos
}

func (s *StateTracker) UpdateLastResumableSourceBinlogPositionForInlineVerifier(pos mysql.Position) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	s.lastStoredBinlogPositionForInlineVerifier = pos
}

func (s *StateTracker) UpdateLastResumableBinlogPositionForTargetVerifier(pos mysql.Position) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	s.lastStoredBinlogPositionForTargetVerifier = pos
}

// Coordinate-based accessors and mutators.
//
// These are the forward-looking API. For file/position coordinates they store
// into the existing file/position fields (unchanged behavior). For GTID
// coordinates they store into dedicated GTID fields, so both representations
// can coexist and the read path is selected by coordinate mode.

func (s *StateTracker) UpdateLastResumableSourceBinlogCoordinate(coord BinlogCoordinate) {
	if coord.IsGTID() {
		s.BinlogRWMutex.Lock()
		defer s.BinlogRWMutex.Unlock()
		c := coord
		s.lastWrittenBinlogCoordinate = &c
		return
	}
	s.UpdateLastResumableSourceBinlogPosition(coord.Position())
}

func (s *StateTracker) UpdateLastResumableSourceBinlogCoordinateForInlineVerifier(coord BinlogCoordinate) {
	if coord.IsGTID() {
		s.BinlogRWMutex.Lock()
		defer s.BinlogRWMutex.Unlock()
		c := coord
		s.lastStoredBinlogCoordinateForInlineVerifier = &c
		return
	}
	s.UpdateLastResumableSourceBinlogPositionForInlineVerifier(coord.Position())
}

func (s *StateTracker) UpdateLastResumableBinlogCoordinateForTargetVerifier(coord BinlogCoordinate) {
	if coord.IsGTID() {
		s.BinlogRWMutex.Lock()
		defer s.BinlogRWMutex.Unlock()
		c := coord
		s.lastStoredBinlogCoordinateForTargetVerifier = &c
		return
	}
	s.UpdateLastResumableBinlogPositionForTargetVerifier(coord.Position())
}

func (s *StateTracker) LastResumableSourceBinlogCoordinate() BinlogCoordinate {
	s.BinlogRWMutex.RLock()
	defer s.BinlogRWMutex.RUnlock()

	if s.lastWrittenBinlogCoordinate != nil {
		return *s.lastWrittenBinlogCoordinate
	}
	return NewFilePositionCoordinate(s.lastWrittenBinlogPosition)
}

func (s *StateTracker) LastResumableSourceBinlogCoordinateForInlineVerifier() BinlogCoordinate {
	s.BinlogRWMutex.RLock()
	defer s.BinlogRWMutex.RUnlock()

	if s.lastStoredBinlogCoordinateForInlineVerifier != nil {
		return *s.lastStoredBinlogCoordinateForInlineVerifier
	}
	return NewFilePositionCoordinate(s.lastStoredBinlogPositionForInlineVerifier)
}

func (s *StateTracker) LastResumableBinlogCoordinateForTargetVerifier() BinlogCoordinate {
	s.BinlogRWMutex.RLock()
	defer s.BinlogRWMutex.RUnlock()

	if s.lastStoredBinlogCoordinateForTargetVerifier != nil {
		return *s.lastStoredBinlogCoordinateForTargetVerifier
	}
	return NewFilePositionCoordinate(s.lastStoredBinlogPositionForTargetVerifier)
}

func (s *StateTracker) UpdateLastSuccessfulPaginationKey(table string, paginationKey PaginationKey, rowStats RowStats) {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	var deltaPaginationKey float64
	if lastKey, exists := s.lastSuccessfulPaginationKeys[table]; exists {
		deltaPaginationKey = paginationKey.NumericPosition() - lastKey.NumericPosition()
	} else {
		deltaPaginationKey = paginationKey.NumericPosition()
	}
	s.lastSuccessfulPaginationKeys[table] = paginationKey

	// TODO: this code is intentionally left here so it is kind of crappy and
	// hopefully will motivate us to fix it by refactoring the state tracker a bit
	// in the future. Namely, the tracking of performance metrics and the tracking
	// of pagination key locations should be done more separately than it is now.
	s.updateRowStatsForTable(table, rowStats)

	s.updateSpeedLog(deltaPaginationKey)
}

func (s *StateTracker) RowStatsWrittenPerTable() map[string]RowStats {
	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	d := make(map[string]RowStats)
	for k, v := range s.rowStatsWrittenPerTable {
		d[k] = v
	}

	return d
}

func (s *StateTracker) LastSuccessfulPaginationKey(table string, tableSchema *TableSchema) PaginationKey {
	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	_, found := s.completedTables[table]
	if found {
		return MaxPaginationKey(tableSchema.GetPaginationColumn())
	}

	paginationKey, found := s.lastSuccessfulPaginationKeys[table]
	if !found {
		return MinPaginationKey(tableSchema.GetPaginationColumn())
	}

	return paginationKey
}

func (s *StateTracker) MarkTableAsCompleted(table string) {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	s.completedTables[table] = true
}

func (s *StateTracker) IsTableComplete(table string) bool {
	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	return s.completedTables[table]
}

// This is reasonably accurate if the rows copied are distributed uniformly
// between paginationKey = 0 -> max(paginationKey). It would not be accurate if the distribution is
// concentrated in a particular region.
func (s *StateTracker) EstimatedPaginationKeysPerSecond() float64 {
	if s.iterationSpeedLog == nil {
		return 0.0
	}

	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	if s.iterationSpeedLog.Value.(PaginationKeyPositionLog).Position == 0 {
		return 0.0
	}

	earliest := s.iterationSpeedLog
	for earliest.Prev() != nil && earliest.Prev() != s.iterationSpeedLog && earliest.Prev().Value.(PaginationKeyPositionLog).Position != 0 {
		earliest = earliest.Prev()
	}

	currentValue := s.iterationSpeedLog.Value.(PaginationKeyPositionLog)
	earliestValue := earliest.Value.(PaginationKeyPositionLog)
	deltaPaginationKey := currentValue.Position - earliestValue.Position
	deltaT := currentValue.At.Sub(earliestValue.At).Seconds()

	return float64(deltaPaginationKey) / deltaT
}

func (s *StateTracker) updateRowStatsForTable(table string, rowStats RowStats) {
	s.rowStatsWrittenPerTable[table] = RowStats{
		NumBytes: rowStats.NumBytes + s.rowStatsWrittenPerTable[table].NumBytes,
		NumRows:  rowStats.NumRows + s.rowStatsWrittenPerTable[table].NumRows,
	}
}

func (s *StateTracker) updateSpeedLog(deltaPaginationKey float64) {
	if s.iterationSpeedLog == nil {
		return
	}

	currentTotalPaginationKey := s.iterationSpeedLog.Value.(PaginationKeyPositionLog).Position
	s.iterationSpeedLog = s.iterationSpeedLog.Next()
	s.iterationSpeedLog.Value = PaginationKeyPositionLog{
		Position: currentTotalPaginationKey + deltaPaginationKey,
		At:       time.Now(),
	}
}

func (s *StateTracker) Serialize(lastKnownTableSchemaCache TableSchemaCache, binlogVerifyStore *BinlogVerifyStore) *SerializableState {
	s.BinlogRWMutex.RLock()
	defer s.BinlogRWMutex.RUnlock()

	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	state := &SerializableState{
		GhostferryVersion:                         VersionString,
		LastKnownTableSchemaCache:                 lastKnownTableSchemaCache,
		LastSuccessfulPaginationKeys:              make(map[string]PaginationKey),
		CompletedTables:                           make(map[string]bool),
		LastWrittenBinlogPosition:                 s.lastWrittenBinlogPosition,
		LastStoredBinlogPositionForInlineVerifier: s.lastStoredBinlogPositionForInlineVerifier,
		LastStoredBinlogPositionForTargetVerifier: s.lastStoredBinlogPositionForTargetVerifier,

		LastWrittenBinlogCoordinate:                 s.lastWrittenBinlogCoordinate,
		LastStoredBinlogCoordinateForInlineVerifier: s.lastStoredBinlogCoordinateForInlineVerifier,
		LastStoredBinlogCoordinateForTargetVerifier: s.lastStoredBinlogCoordinateForTargetVerifier,
	}

	// If any GTID coordinate has been recorded, this state was produced in GTID
	// mode. Marking the mode lets readers select the GTID fields on resume.
	if s.lastWrittenBinlogCoordinate != nil ||
		s.lastStoredBinlogCoordinateForInlineVerifier != nil ||
		s.lastStoredBinlogCoordinateForTargetVerifier != nil {
		state.BinlogCoordinateMode = BinlogCoordinateGTID
	}

	if binlogVerifyStore != nil {
		state.BinlogVerifyStore = binlogVerifyStore.Serialize()
	}

	// Need a copy because lastSuccessfulPaginationKeys may change after Serialize
	// returns. This would inaccurately reflect the state of Ghostferry when
	// Serialize is called.
	for k, v := range s.lastSuccessfulPaginationKeys {
		state.LastSuccessfulPaginationKeys[k] = v
	}

	for k, v := range s.completedTables {
		state.CompletedTables[k] = v
	}

	return state
}
