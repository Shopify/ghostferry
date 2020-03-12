package ghostferry

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/siddontang/go-mysql/mysql"
)

// NOTE: This file is only used for the ControlServer for now.
// TODO: eventually merge this into the ControlServer and use the Progress struct.

type TableStatusDeprecated struct {
	TableName                   string
	PaginationKeyName           string
	Status                      string
	LastSuccessfulPaginationKey uint64
	TargetPaginationKey         uint64
}

type StatusDeprecated struct {
	GhostferryVersion string

	SourceHostPort string
	TargetHostPort string

	OverallState            string
	StartTime               time.Time
	CurrentTime             time.Time
	TimeTaken               time.Duration
	ETA                     time.Duration
	BinlogStreamerLag       time.Duration
	PaginationKeysPerSecond uint64

	AutomaticCutover            bool
	BinlogStreamerStopRequested bool
	LastSuccessfulBinlogPos     mysql.Position
	TargetBinlogPos             mysql.Position

	Throttled bool

	CompletedTableCount int
	TotalTableCount     int
	TableStatuses       []*TableStatusDeprecated
	AllTableNames       []string
	AllDatabaseNames    []string

	VerifierSupport     bool
	VerifierAvailable   bool
	VerificationStarted bool
	VerificationDone    bool
	VerificationResult  VerificationResult
	VerificationErr     error
}

func FetchStatusDeprecated(f *Ferry, v Verifier) *StatusDeprecated {
	status := &StatusDeprecated{}

	status.GhostferryVersion = VersionString

	status.SourceHostPort = fmt.Sprintf("%s:%d", f.Source.Host, f.Source.Port)
	status.TargetHostPort = fmt.Sprintf("%s:%d", f.Target.Host, f.Target.Port)

	status.OverallState = f.OverallState
	status.StartTime = f.StartTime
	status.CurrentTime = time.Now()
	if f.DoneTime.IsZero() {
		status.TimeTaken = status.CurrentTime.Sub(status.StartTime)
	} else {
		status.TimeTaken = f.DoneTime.Sub(status.StartTime)
	}
	status.BinlogStreamerLag = time.Now().Sub(f.BinlogStreamer.lastProcessedEventTime)

	status.AutomaticCutover = f.Config.AutomaticCutover
	status.BinlogStreamerStopRequested = f.BinlogStreamer.stopRequested
	status.LastSuccessfulBinlogPos = f.BinlogStreamer.GetLastStreamedBinlogPosition()
	status.TargetBinlogPos = f.BinlogStreamer.targetBinlogPosition

	status.Throttled = f.Throttler.Throttled()

	// Getting all table statuses
	status.TableStatuses = make([]*TableStatusDeprecated, 0, len(f.Tables))

	serializedState := f.StateTracker.Serialize(nil, nil)

	lastSuccessfulPaginationKeys := serializedState.LastSuccessfulPaginationKeys
	completedTables := serializedState.CompletedTables

	targetPaginationKeys := make(map[string]uint64)
	f.DataIterator.targetPaginationKeys.Range(func(k, v interface{}) bool {
		targetPaginationKeys[k.(string)] = v.(uint64)
		return true
	})

	status.CompletedTableCount = len(completedTables)
	status.TotalTableCount = len(f.Tables)

	status.AllTableNames = f.Tables.AllTableNames()
	sort.Strings(status.AllTableNames)

	dbSet := make(map[string]bool)
	for _, table := range f.Tables.AsSlice() {
		dbSet[table.Schema] = true
	}

	status.AllDatabaseNames = make([]string, 0, len(dbSet))
	for dbName := range dbSet {
		status.AllDatabaseNames = append(status.AllDatabaseNames, dbName)
	}
	sort.Strings(status.AllDatabaseNames)

	// We get the name first because we need to sort them
	completedTableNames := make([]string, 0, len(completedTables))
	copyingTableNames := make([]string, 0, len(f.Tables))
	waitingTableNames := make([]string, 0, len(f.Tables))

	for tableName, _ := range completedTables {
		completedTableNames = append(completedTableNames, tableName)
	}

	for tableName, _ := range lastSuccessfulPaginationKeys {
		if _, ok := completedTables[tableName]; ok {
			continue // already completed, therefore not copying
		}

		copyingTableNames = append(copyingTableNames, tableName)
	}

	for tableName, _ := range f.Tables {
		if lastSuccessfulPaginationKey, ok := lastSuccessfulPaginationKeys[tableName]; ok && lastSuccessfulPaginationKey != 0 {
			continue // already started, therefore not waiting
		}

		if _, ok := completedTables[tableName]; ok {
			// There are no data in that table, thus it does not have an entry in
			// lastSuccessfulPaginationKeys but has an entry in completedTables
			continue
		}

		waitingTableNames = append(waitingTableNames, tableName)
	}

	sort.Strings(completedTableNames)
	sort.Strings(copyingTableNames)
	sort.Strings(waitingTableNames)

	for _, tableName := range completedTableNames {
		status.TableStatuses = append(status.TableStatuses, &TableStatusDeprecated{
			TableName:                   tableName,
			PaginationKeyName:           f.Tables[tableName].GetPaginationColumn().Name,
			Status:                      "complete",
			TargetPaginationKey:         targetPaginationKeys[tableName],
			LastSuccessfulPaginationKey: lastSuccessfulPaginationKeys[tableName],
		})
	}

	for _, tableName := range copyingTableNames {
		status.TableStatuses = append(status.TableStatuses, &TableStatusDeprecated{
			TableName:                   tableName,
			PaginationKeyName:           f.Tables[tableName].GetPaginationColumn().Name,
			Status:                      "copying",
			TargetPaginationKey:         targetPaginationKeys[tableName],
			LastSuccessfulPaginationKey: lastSuccessfulPaginationKeys[tableName],
		})
	}

	for _, tableName := range waitingTableNames {
		status.TableStatuses = append(status.TableStatuses, &TableStatusDeprecated{
			TableName:                   tableName,
			PaginationKeyName:           f.Tables[tableName].GetPaginationColumn().Name,
			Status:                      "waiting",
			TargetPaginationKey:         targetPaginationKeys[tableName],
			LastSuccessfulPaginationKey: 0,
		})
	}

	// ETA estimation
	// We do it here rather than in DataIteratorState to give the lock back
	// ASAP. It's not supposed to be that accurate anyway.
	var totalPaginationKeysToCopy uint64 = 0
	var completedPaginationKeys uint64 = 0
	estimatedPaginationKeysPerSecond := f.StateTracker.EstimatedPaginationKeysPerSecond()
	for _, targetPaginationKey := range targetPaginationKeys {
		totalPaginationKeysToCopy += targetPaginationKey
	}

	for _, completedPaginationKey := range lastSuccessfulPaginationKeys {
		completedPaginationKeys += completedPaginationKey
	}

	status.ETA = time.Duration(math.Ceil(float64(totalPaginationKeysToCopy-completedPaginationKeys)/estimatedPaginationKeysPerSecond)) * time.Second
	status.PaginationKeysPerSecond = uint64(estimatedPaginationKeysPerSecond)

	// Verifier display
	if v != nil {
		status.VerifierSupport = true

		result, err := v.Result()
		status.VerificationStarted = result.IsStarted()
		status.VerificationDone = result.IsDone()

		// We can only run the verifier if we're not copying and not verifying
		status.VerifierAvailable = status.OverallState != StateStarting && status.OverallState != StateCopying && (!status.VerificationStarted || status.VerificationDone)
		status.VerificationResult = result.VerificationResult
		status.VerificationErr = err
	} else {
		status.VerifierSupport = false
		status.VerifierAvailable = false
	}

	return status
}
