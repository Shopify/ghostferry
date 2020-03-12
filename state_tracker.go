package ghostferry

import (
	"container/ring"
	"math"
	"sync"
	"time"
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

	LastSuccessfulPaginationKeys              map[string]uint64
	CompletedTables                           map[string]bool
	LastWrittenBinlogPosition                 BinlogPosition
	LastStoredBinlogPositionForInlineVerifier BinlogPosition
	BinlogVerifyStore                         BinlogVerifySerializedStore
}

func (s *SerializableState) MinBinlogPosition() BinlogPosition {
	nilPosition := BinlogPosition{}
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

// For tracking the speed of the copy
type PaginationKeyPositionLog struct {
	Position uint64
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

type StateTracker struct {
	BinlogRWMutex *sync.RWMutex
	CopyRWMutex   *sync.RWMutex

	lastWrittenBinlogPosition                 BinlogPosition
	lastStoredBinlogPositionForInlineVerifier BinlogPosition

	lastSuccessfulPaginationKeys map[string]uint64
	completedTables              map[string]bool

	iterationSpeedLog *ring.Ring
}

func NewStateTracker(speedLogCount int) *StateTracker {
	return &StateTracker{
		BinlogRWMutex: &sync.RWMutex{},
		CopyRWMutex:   &sync.RWMutex{},

		lastSuccessfulPaginationKeys: make(map[string]uint64),
		completedTables:              make(map[string]bool),
		iterationSpeedLog:            newSpeedLogRing(speedLogCount),
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
	return s
}

func (s *StateTracker) UpdateLastWrittenBinlogPosition(pos BinlogPosition) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	s.lastWrittenBinlogPosition = pos
}

func (s *StateTracker) UpdateLastStoredBinlogPositionForInlineVerifier(pos BinlogPosition) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	s.lastStoredBinlogPositionForInlineVerifier = pos
}

func (s *StateTracker) UpdateLastSuccessfulPaginationKey(table string, paginationKey uint64) {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	deltaPaginationKey := paginationKey - s.lastSuccessfulPaginationKeys[table]
	s.lastSuccessfulPaginationKeys[table] = paginationKey

	s.updateSpeedLog(deltaPaginationKey)
}

func (s *StateTracker) LastSuccessfulPaginationKey(table string) uint64 {
	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	_, found := s.completedTables[table]
	if found {
		return math.MaxUint64
	}

	paginationKey, found := s.lastSuccessfulPaginationKeys[table]
	if !found {
		return 0
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

func (s *StateTracker) updateSpeedLog(deltaPaginationKey uint64) {
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
		LastSuccessfulPaginationKeys:              make(map[string]uint64),
		CompletedTables:                           make(map[string]bool),
		LastWrittenBinlogPosition:                 s.lastWrittenBinlogPosition,
		LastStoredBinlogPositionForInlineVerifier: s.lastStoredBinlogPositionForInlineVerifier,
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
