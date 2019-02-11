package ghostferry

import (
	"container/ring"
	"math"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/mysql"
)

// StateTracker design
// ===================
//
// General Overview
// ----------------
//
// The state tracker keeps track of the progress of Ghostferry so it can be
// interrupted and resumed. The state tracker is supposed to be initialized and
// managed by the Ferry. Each Ghostferry components, such as the `BatchWriter`
// and `IterativeVerifier` will get passed an instance of either the
// `CopyStateTracker` or the `VerificationStateTracker`. During the run, these
// components will update their last successful components to the state tracker
// instances given via the state tracker API defined here.
//
// The states stored in the state tracker can be copied into a serialization
// friendly struct (`SerializableState`), which can then be dumped using
// something like JSON. Assuming the rest of Ghostferry used the API of the
// state tracker correctly, this can be done at any point during the Ghostferry
// run and the resulting state is can be resumed from without incurring data
// loss. The same `SerializableState` can be used as an input to `Ferry`, which
// if specified will instruct to `Ferry` to start from a particular location.
//
// Code Design
// -----------
//
// In a Ghostferry run, there are two "stages" of operation: the copy stage and
// the verification stage. Both stages must emit their states to the state
// tracker in order for them to be interruptible and resumable. These two
// stages are very similar: they both iterate over the data and tail the
// binlog. However, there are some minor differences. Example: the verifier
// stage needs to keep track of the reverify store while the copy stage
// doesn't.
//
// Note that the copy stage is always active during a run as we need to copy
// binlog data to the target. This means both the copy and the verification
// stage can be active at the same time.
//
// In order to not repeat code, "base structs" are created for the state
// tracker and the serializable state: `BinlogStateTracker`,
// `BinlogSerializableState`, `IterationStateTracker`,
// `IterationSerializableState`. These are internal structs that are used to
// share code and definitions for the `CopyStateTracker` and the
// `VerificationStateTracker`. The state trackers and serializable states for
// the actual stages contain a minor amount of customized code in order to
// implement the distinct behaviours required by the stages.
//
// These two stages of state tracker (and serializable states) are owned by a
// "global" `StateTracker`. This struct keeps two member variables pointing to
// the state tracker of each stage and that's it. The Ferry creates a new
// instance of the global `StateTracker` and passes out references of the
// applicable stage of state tracker to components like `BatchWriter` and
// `IterativeVerifier`.
//
// To summarize, what we have is (arrows point from member variables to owner
// structs):
//
//  BinlogStateTracker       BinlogStateTracker
// IterationStateTracker   IterationStateTracker
//          |                        |
//          v                        v
//   CopyStateTracker       VerifierStateTracker
//          |                        |
//          +-----------+------------+
//                      |
//                      v
//                 StateTracker
//                      |
//                      v
//                    Ferry
//
// The same relationship exists for the serializable states.

type BinlogSerializableState struct {
	LastProcessedBinlogPosition mysql.Position
}

type IterationSerializableState struct {
	LastSuccessfulPrimaryKeys map[string]uint64
	CompletedTables           map[string]bool
}

type CopySerializableState struct {
	*BinlogSerializableState
	*IterationSerializableState
}

type VerificationSerializableState struct {
	*BinlogSerializableState
	*IterationSerializableState

	// This is not efficient because we have to build this map of a different
	// type from the original ReverifyStore struct.
	//
	// TODO: address this inefficiency later.
	ReverifyStore map[string][]uint64
}

const (
	StageCopy         = "COPY"
	StageVerification = "VERIFICATION"
)

// This is the struct that is dumped by Ghostferry when it is interrupted. It
// is the same struct that is given to Ghostferry when it is resumed.
type SerializableState struct {
	GhostferryVersion         string
	LastKnownTableSchemaCache TableSchemaCache

	CurrentStage  string
	CopyStage     *CopySerializableState
	VerifierStage *VerificationSerializableState
}

// The binlog writer and the verify binlog positions are different because the
// binlog writer is buffered in a background go routine. Its position can race
// with respect to the verifier binlog position. The minimum position between
// the two are always safe to resume from.
func (s *SerializableState) MinBinlogPosition() mysql.Position {
	if s.VerifierStage == nil {
		return s.CopyStage.LastProcessedBinlogPosition
	}

	c := s.CopyStage.LastProcessedBinlogPosition.Compare(s.VerifierStage.LastProcessedBinlogPosition)

	if c >= 0 {
		return s.CopyStage.LastProcessedBinlogPosition
	} else {
		return s.VerifierStage.LastProcessedBinlogPosition
	}
}

// For tracking the speed of the copy
type PKPositionLog struct {
	Position uint64
	At       time.Time
}

func newSpeedLogRing(speedLogCount int) *ring.Ring {
	if speedLogCount <= 0 {
		return nil
	}

	speedLog := ring.New(speedLogCount)
	speedLog.Value = PKPositionLog{
		Position: 0,
		At:       time.Now(),
	}

	return speedLog
}

type BinlogStateTracker struct {
	*sync.RWMutex
	lastProcessedBinlogPosition mysql.Position
}

func NewBinlogStateTracker() *BinlogStateTracker {
	return &BinlogStateTracker{
		RWMutex:                     &sync.RWMutex{},
		lastProcessedBinlogPosition: mysql.Position{},
	}
}

func (s *BinlogStateTracker) UpdateLastProcessedBinlogPosition(pos mysql.Position) {
	s.Lock()
	defer s.Unlock()

	s.lastProcessedBinlogPosition = pos
}

func (s *BinlogStateTracker) Serialize() *BinlogSerializableState {
	s.RLock()
	defer s.RUnlock()

	return &BinlogSerializableState{
		LastProcessedBinlogPosition: s.lastProcessedBinlogPosition,
	}
}

type IterationStateTracker struct {
	*sync.RWMutex

	lastSuccessfulPrimaryKeys map[string]uint64
	completedTables           map[string]bool

	iterationSpeedLog *ring.Ring
}

func NewIterationStateTracker(speedLogCount int) *IterationStateTracker {
	return &IterationStateTracker{
		RWMutex:                   &sync.RWMutex{},
		lastSuccessfulPrimaryKeys: make(map[string]uint64),
		completedTables:           make(map[string]bool),
		iterationSpeedLog:         newSpeedLogRing(speedLogCount),
	}
}

func (s *IterationStateTracker) UpdateLastSuccessfulPK(table string, pk uint64) {
	s.Lock()
	defer s.Unlock()

	deltaPK := pk - s.lastSuccessfulPrimaryKeys[table]
	s.lastSuccessfulPrimaryKeys[table] = pk

	s.updateSpeedLog(deltaPK)
}

func (s *IterationStateTracker) LastSuccessfulPK(table string) uint64 {
	s.RLock()
	defer s.RUnlock()

	_, found := s.completedTables[table]
	if found {
		return math.MaxUint64
	}

	pk, found := s.lastSuccessfulPrimaryKeys[table]
	if !found {
		return 0
	}

	return pk
}

func (s *IterationStateTracker) MarkTableAsCompleted(table string) {
	s.Lock()
	defer s.Unlock()

	s.completedTables[table] = true
}

func (s *IterationStateTracker) IsTableComplete(table string) bool {
	s.RLock()
	defer s.RUnlock()

	return s.completedTables[table]
}

func (s *IterationStateTracker) Serialize() *IterationSerializableState {
	s.RLock()
	defer s.RUnlock()

	state := &IterationSerializableState{
		LastSuccessfulPrimaryKeys: make(map[string]uint64),
		CompletedTables:           make(map[string]bool),
	}

	for k, v := range s.lastSuccessfulPrimaryKeys {
		state.LastSuccessfulPrimaryKeys[k] = v
	}

	for k, v := range s.completedTables {
		state.CompletedTables[k] = v
	}

	return state
}

// This is reasonably accurate if the rows copied are distributed uniformly
// between pk = 0 -> max(pk). It would not be accurate if the distribution is
// concentrated in a particular region.
func (s *IterationStateTracker) EstimatedPKsPerSecond() float64 {
	if s.iterationSpeedLog == nil {
		return 0.0
	}

	s.RLock()
	defer s.RUnlock()

	if s.iterationSpeedLog.Value.(PKPositionLog).Position == 0 {
		return 0.0
	}

	earliest := s.iterationSpeedLog
	for earliest.Prev() != nil && earliest.Prev() != s.iterationSpeedLog && earliest.Prev().Value.(PKPositionLog).Position != 0 {
		earliest = earliest.Prev()
	}

	currentValue := s.iterationSpeedLog.Value.(PKPositionLog)
	earliestValue := earliest.Value.(PKPositionLog)
	deltaPK := currentValue.Position - earliestValue.Position
	deltaT := currentValue.At.Sub(earliestValue.At).Seconds()

	return float64(deltaPK) / deltaT
}

func (s *IterationStateTracker) updateSpeedLog(deltaPK uint64) {
	if s.iterationSpeedLog == nil {
		return
	}

	currentTotalPK := s.iterationSpeedLog.Value.(PKPositionLog).Position
	s.iterationSpeedLog = s.iterationSpeedLog.Next()
	s.iterationSpeedLog.Value = PKPositionLog{
		Position: currentTotalPK + deltaPK,
		At:       time.Now(),
	}
}

type CopyStateTracker struct {
	*BinlogStateTracker
	*IterationStateTracker
}

func (s *CopyStateTracker) Serialize() *CopySerializableState {
	return &CopySerializableState{
		BinlogSerializableState:    s.BinlogStateTracker.Serialize(),
		IterationSerializableState: s.IterationStateTracker.Serialize(),
	}
}

func NewCopyStateTracker(speedLogCount int) *CopyStateTracker {
	return &CopyStateTracker{
		BinlogStateTracker:    NewBinlogStateTracker(),
		IterationStateTracker: NewIterationStateTracker(speedLogCount),
	}
}

type VerificationStateTracker struct {
	*BinlogStateTracker
	*IterationStateTracker
	// TODO: this struct needs to keep track of the reverify store and dump it
	//       with Serialize.
}

func (s *VerificationStateTracker) Serialize() *VerificationSerializableState {
	// TODO: this method needs to dump the reverify store.
	return &VerificationSerializableState{
		BinlogSerializableState:    s.BinlogStateTracker.Serialize(),
		IterationSerializableState: s.IterationStateTracker.Serialize(),
	}
}

type StateTracker struct {
	CopyStage *CopyStateTracker
	// TODO: implement this
	// VerificationStage *VerifierStateTracker
}

// speedLogCount should be a number that is an order of magnitude or so larger
// than the number of table iterators. This is to ensure the ring buffer used
// to calculate the speed is not filled with only data from the last iteration
// of the cursor and thus would be wildly inaccurate.
func NewStateTracker(speedLogCount int) *StateTracker {
	return &StateTracker{
		CopyStage: NewCopyStateTracker(speedLogCount),
	}
}

// serializedState is a state the tracker should start from, as opposed to
// starting from the beginning.
func NewStateTrackerFromSerializedState(speedLogCount int, serializedState *SerializableState) *StateTracker {
	s := NewStateTracker(speedLogCount)
	s.CopyStage.lastSuccessfulPrimaryKeys = serializedState.CopyStage.LastSuccessfulPrimaryKeys
	s.CopyStage.completedTables = serializedState.CopyStage.CompletedTables
	s.CopyStage.lastProcessedBinlogPosition = serializedState.CopyStage.LastProcessedBinlogPosition
	return s
}

func (s *StateTracker) Serialize(lastKnownTableSchemaCache TableSchemaCache) *SerializableState {
	return &SerializableState{
		GhostferryVersion:         VersionString,
		LastKnownTableSchemaCache: lastKnownTableSchemaCache,

		// TODO: implement verifier stage
		CurrentStage: StageCopy,
		CopyStage:    s.CopyStage.Serialize(),
	}
}
