package ghostferry

import (
	"container/ring"
	"encoding/json"
	"fmt"
	"math"
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

	BinlogCoordinateMode                        BinlogCoordinateType `json:",omitempty"`
	LastWrittenBinlogCoordinate                 *BinlogCoordinate    `json:",omitempty"`
	LastStoredBinlogCoordinateForInlineVerifier *BinlogCoordinate    `json:",omitempty"`
	LastStoredBinlogCoordinateForTargetVerifier *BinlogCoordinate    `json:",omitempty"`
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

// CoordinateMode returns the effective mode, treating empty as file/position (legacy states).
func (s *SerializableState) CoordinateMode() BinlogCoordinateType {
	if s.BinlogCoordinateMode == "" {
		return BinlogCoordinateFilePosition
	}
	return s.BinlogCoordinateMode
}

func (s *SerializableState) coordinateMode() BinlogCoordinateType {
	return s.CoordinateMode()
}

// HasTargetVerifierBinlogCoordinate reports presence, not non-zero-ness: in
// GTID mode an empty executed set is a valid coordinate.
func (s *SerializableState) HasTargetVerifierBinlogCoordinate() bool {
	if s.coordinateMode() == BinlogCoordinateGTID {
		return s.LastStoredBinlogCoordinateForTargetVerifier != nil
	}
	return s.LastStoredBinlogPositionForTargetVerifier != (mysql.Position{})
}

func (s *SerializableState) WrittenSourceBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID && s.LastWrittenBinlogCoordinate != nil {
		return *s.LastWrittenBinlogCoordinate
	}
	return NewFilePositionCoordinate(s.LastWrittenBinlogPosition)
}

func (s *SerializableState) InlineVerifierSourceBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID && s.LastStoredBinlogCoordinateForInlineVerifier != nil {
		return *s.LastStoredBinlogCoordinateForInlineVerifier
	}
	return NewFilePositionCoordinate(s.LastStoredBinlogPositionForInlineVerifier)
}

func (s *SerializableState) TargetVerifierBinlogCoordinate() BinlogCoordinate {
	if s.coordinateMode() == BinlogCoordinateGTID && s.LastStoredBinlogCoordinateForTargetVerifier != nil {
		return *s.LastStoredBinlogCoordinateForTargetVerifier
	}
	return NewFilePositionCoordinate(s.LastStoredBinlogPositionForTargetVerifier)
}

// MinSourceBinlogCoordinate returns the safe source resume coordinate. In GTID
// mode that is the intersection (or sole side) of the writer and inline-verifier
// sets; failures return an error rather than a too-advanced resume floor.
func (s *SerializableState) MinSourceBinlogCoordinate() (BinlogCoordinate, error) {
	if s.coordinateMode() != BinlogCoordinateGTID {
		return NewFilePositionCoordinate(s.MinSourceBinlogPosition()), nil
	}

	written := s.LastWrittenBinlogCoordinate
	inline := s.LastStoredBinlogCoordinateForInlineVerifier

	if written == nil && inline == nil {
		return NewGTIDCoordinate(""), nil
	}
	if written == nil {
		return *inline, nil
	}
	if inline == nil {
		return *written, nil
	}

	writtenSet, err := written.ParsedGTIDSet()
	if err != nil {
		return BinlogCoordinate{}, fmt.Errorf("parsing written GTID coordinate %q: %w", written.GTIDSet, err)
	}
	inlineSet, err := inline.ParsedGTIDSet()
	if err != nil {
		return BinlogCoordinate{}, fmt.Errorf("parsing inline-verifier GTID coordinate %q: %w", inline.GTIDSet, err)
	}

	intersection, err := intersectGTIDSets(writtenSet, inlineSet)
	if err != nil {
		return BinlogCoordinate{}, fmt.Errorf("computing safe GTID resume floor: %w", err)
	}
	return NewGTIDCoordinateFromSet(intersection), nil
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

func (s *StateTracker) UpdateLastResumableSourceBinlogCoordinate(coord BinlogCoordinate) {
	if coord.IsGTID() {
		s.storeGTIDCoordinate(&s.lastWrittenBinlogCoordinate, coord)
		return
	}
	s.UpdateLastResumableSourceBinlogPosition(coord.Position())
}

func (s *StateTracker) UpdateLastResumableSourceBinlogCoordinateForInlineVerifier(coord BinlogCoordinate) {
	if coord.IsGTID() {
		s.storeGTIDCoordinate(&s.lastStoredBinlogCoordinateForInlineVerifier, coord)
		return
	}
	s.UpdateLastResumableSourceBinlogPositionForInlineVerifier(coord.Position())
}

func (s *StateTracker) UpdateLastResumableBinlogCoordinateForTargetVerifier(coord BinlogCoordinate) {
	if coord.IsGTID() {
		s.storeGTIDCoordinate(&s.lastStoredBinlogCoordinateForTargetVerifier, coord)
		return
	}
	s.UpdateLastResumableBinlogPositionForTargetVerifier(coord.Position())
}

func (s *StateTracker) storeGTIDCoordinate(dst **BinlogCoordinate, coord BinlogCoordinate) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	*dst = &coord
}

func (s *StateTracker) LastResumableSourceBinlogCoordinate() BinlogCoordinate {
	return s.gtidCoordinateOrPosition(&s.lastWrittenBinlogCoordinate, &s.lastWrittenBinlogPosition)
}

func (s *StateTracker) LastResumableSourceBinlogCoordinateForInlineVerifier() BinlogCoordinate {
	return s.gtidCoordinateOrPosition(&s.lastStoredBinlogCoordinateForInlineVerifier, &s.lastStoredBinlogPositionForInlineVerifier)
}

func (s *StateTracker) LastResumableBinlogCoordinateForTargetVerifier() BinlogCoordinate {
	return s.gtidCoordinateOrPosition(&s.lastStoredBinlogCoordinateForTargetVerifier, &s.lastStoredBinlogPositionForTargetVerifier)
}

func (s *StateTracker) gtidCoordinateOrPosition(src **BinlogCoordinate, pos *mysql.Position) BinlogCoordinate {
	s.BinlogRWMutex.RLock()
	defer s.BinlogRWMutex.RUnlock()

	if *src != nil {
		return **src
	}
	return NewFilePositionCoordinate(*pos)
}

func (s *StateTracker) UpdateLastSuccessfulPaginationKey(table string, paginationKey PaginationKey, rowStats RowStats) {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	var deltaPaginationKey float64
	if lastKey, exists := s.lastSuccessfulPaginationKeys[table]; exists {
		deltaPaginationKey = paginationKey.NumericPosition() - lastKey.NumericPosition()
	} else {
		// There is no known starting position for the first batch of negative
		// keys. Measure subsequent batches instead of recording negative speed.
		deltaPaginationKey = math.Max(0, paginationKey.NumericPosition())
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

// LastSuccessfulPaginationKey returns nil for an unstarted table. Completed
// tables retain their maximum sentinel; real checkpoints can have any key value.
func (s *StateTracker) LastSuccessfulPaginationKey(table string, tableSchema *TableSchema) PaginationKey {
	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	_, found := s.completedTables[table]
	if found {
		return MaxPaginationKey(tableSchema.GetPaginationColumn())
	}

	return s.lastSuccessfulPaginationKeys[table]
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
	if deltaT <= 0 {
		return 0
	}

	return math.Max(0, deltaPaginationKey/deltaT)
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
