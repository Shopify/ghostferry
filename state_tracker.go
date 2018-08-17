package ghostferry

import (
	"container/ring"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/mysql"
)

type SerializableState struct {
	GhostferryVersion         string
	LastKnownTableSchemaCache TableSchemaCache

	LastSuccessfulPrimaryKeys  map[string]uint64
	CompletedTables            map[string]bool
	LastStreamedBinlogPosition mysql.Position
}

// For tracking the speed of the copy
type PKPositionLog struct {
	Position uint64
	At       time.Time
}

type StateTracker struct {
	lastSuccessfulPrimaryKeys  map[string]uint64
	completedTables            map[string]bool
	lastStreamedBinlogPosition mysql.Position

	binlogMutex *sync.RWMutex
	tableMutex  *sync.RWMutex

	copySpeedLog *ring.Ring
}

func (s *StateTracker) UpdateLastSuccessfulPK(table string, pk uint64) {
	s.tableMutex.Lock()
	defer s.tableMutex.Unlock()

	deltaPK := pk - s.lastSuccessfulPrimaryKeys[table]
	s.lastSuccessfulPrimaryKeys[table] = pk

	currentTotalPK := s.copySpeedLog.Value.(PKPositionLog).Position
	s.copySpeedLog = s.copySpeedLog.Next()
	s.copySpeedLog.Value = PKPositionLog{
		Position: currentTotalPK + deltaPK,
		At:       time.Now(),
	}
}

func (s *StateTracker) MarkTableAsCompleted(table string) {
	s.tableMutex.Lock()
	defer s.tableMutex.Unlock()

	s.completedTables[table] = true
}

func (s *StateTracker) UpdateLastStreamedBinlogPosition(pos mysql.Position) {
	s.binlogMutex.Lock()
	defer s.binlogMutex.Unlock()

	s.lastStreamedBinlogPosition = pos
}

func (s *StateTracker) Serialize(state *SerializableState) {
	s.tableMutex.RLock()
	s.binlogMutex.RLock()
	defer func() {
		s.tableMutex.RUnlock()
		s.binlogMutex.RUnlock()
	}()

	state.LastStreamedBinlogPosition = s.lastStreamedBinlogPosition
	state.LastSuccessfulPrimaryKeys = make(map[string]uint64)
	state.CompletedTables = make(map[string]bool)

	for k, v := range s.lastSuccessfulPrimaryKeys {
		state.LastSuccessfulPrimaryKeys[k] = v
	}

	for k, v := range s.completedTables {
		state.CompletedTables[k] = v
	}
}

// This is reasonably accurate if the rows copied are distributed uniformly
// between pk = 0 -> max(pk). It would not be accurate if the distribution is
// concentrated in a particular region.
func (s *StateTracker) EstimatedPKCopiedPerSecond() float64 {
	s.tableMutex.RLock()
	defer s.tableMutex.RUnlock()

	if s.copySpeedLog.Value.(PKPositionLog).Position == 0 {
		return 0.0
	}

	earliest := s.copySpeedLog
	for earliest.Prev() != nil && earliest.Prev() != s.copySpeedLog && earliest.Prev().Value.(PKPositionLog).Position != 0 {
		earliest = earliest.Prev()
	}

	currentValue := s.copySpeedLog.Value.(PKPositionLog)
	earliestValue := earliest.Value.(PKPositionLog)
	deltaPK := currentValue.Position - earliestValue.Position
	deltaT := currentValue.At.Sub(earliestValue.At).Seconds()

	return float64(deltaPK) / deltaT
}

// speedLogCount should be a number that is an order of magnitude or so larger
// than the number of table iterators. This is to ensure the ring buffer used
// to calculate the speed is not filled with only data from the last iteration
// of the cursor and thus would be wildly inaccurate.
//
// serializedState is a state the tracker should start from, as opposed to
// starting from the beginning.
func NewStateTracker(speedLogCount int, serializedState *SerializableState) *StateTracker {
	if serializedState == nil {
		// A zero MySQL position will be set in the state tracker. The consumer
		// of this struct should realize this means to load the current master
		// position.
		serializedState = &SerializableState{
			LastSuccessfulPrimaryKeys: make(map[string]uint64),
			CompletedTables:           make(map[string]bool),
		}
	}

	speedLog := ring.New(speedLogCount)
	speedLog.Value = PKPositionLog{
		Position: 0,
		At:       time.Now(),
	}

	return &StateTracker{
		lastSuccessfulPrimaryKeys:  serializedState.LastSuccessfulPrimaryKeys,
		completedTables:            serializedState.CompletedTables,
		lastStreamedBinlogPosition: serializedState.LastStreamedBinlogPosition,
		binlogMutex:                &sync.RWMutex{},
		tableMutex:                 &sync.RWMutex{},
		copySpeedLog:               speedLog,
	}
}
