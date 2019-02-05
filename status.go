package ghostferry

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/siddontang/go-mysql/mysql"
)

type TableStatus struct {
	TableName        string
	PrimaryKeyName   string
	Status           string
	LastSuccessfulPK uint64
	TargetPK         uint64
}

type Status struct {
	GhostferryVersion string

	SourceHostPort string
	TargetHostPort string

	OverallState      string
	StartTime         time.Time
	CurrentTime       time.Time
	TimeTaken         time.Duration
	ETA               time.Duration
	BinlogStreamerLag time.Duration
	PKsPerSecond      uint64

	AutomaticCutover            bool
	BinlogStreamerStopRequested bool
	LastSuccessfulBinlogPos     mysql.Position
	TargetBinlogPos             mysql.Position

	Throttled bool

	CompletedTableCount int
	TotalTableCount     int
	TableStatuses       []*TableStatus
	AllTableNames       []string
	AllDatabaseNames    []string

	VerifierSupport     bool
	VerifierAvailable   bool
	VerificationStarted bool
	VerificationDone    bool
	VerificationResult  VerificationResult
	VerificationErr     error
}

func FetchStatus(f *Ferry, v Verifier) *Status {
	status := &Status{}

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
	status.LastSuccessfulBinlogPos = f.BinlogStreamer.lastStreamedBinlogPosition
	status.TargetBinlogPos = f.BinlogStreamer.targetBinlogPosition

	status.Throttled = f.Throttler.Throttled()

	// Getting all table statuses
	status.TableStatuses = make([]*TableStatus, 0, len(f.Tables))

	serializedState := f.StateTracker.Serialize(nil)

	lastSuccessfulPKs := serializedState.CopyStage.LastSuccessfulPrimaryKeys
	completedTables := serializedState.CopyStage.CompletedTables

	targetPKs := make(map[string]uint64)
	f.DataIterator.targetPKs.Range(func(k, v interface{}) bool {
		targetPKs[k.(string)] = v.(uint64)
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

	for tableName, _ := range lastSuccessfulPKs {
		if _, ok := completedTables[tableName]; ok {
			continue // already completed, therefore not copying
		}

		copyingTableNames = append(copyingTableNames, tableName)
	}

	for tableName, _ := range f.Tables {
		if lastSuccessfulPK, ok := lastSuccessfulPKs[tableName]; ok && lastSuccessfulPK != 0 {
			continue // already started, therefore not waiting
		}

		if _, ok := completedTables[tableName]; ok {
			// There are no data in that table, thus it does not have an entry in
			// lastSuccessfulPKs but has an entry in completedTables
			continue
		}

		waitingTableNames = append(waitingTableNames, tableName)
	}

	sort.Strings(completedTableNames)
	sort.Strings(copyingTableNames)
	sort.Strings(waitingTableNames)

	for _, tableName := range completedTableNames {
		status.TableStatuses = append(status.TableStatuses, &TableStatus{
			TableName:        tableName,
			PrimaryKeyName:   f.Tables[tableName].GetPKColumn(0).Name,
			Status:           "complete",
			TargetPK:         targetPKs[tableName],
			LastSuccessfulPK: lastSuccessfulPKs[tableName],
		})
	}

	for _, tableName := range copyingTableNames {
		status.TableStatuses = append(status.TableStatuses, &TableStatus{
			TableName:        tableName,
			PrimaryKeyName:   f.Tables[tableName].GetPKColumn(0).Name,
			Status:           "copying",
			TargetPK:         targetPKs[tableName],
			LastSuccessfulPK: lastSuccessfulPKs[tableName],
		})
	}

	for _, tableName := range waitingTableNames {
		status.TableStatuses = append(status.TableStatuses, &TableStatus{
			TableName:        tableName,
			PrimaryKeyName:   f.Tables[tableName].GetPKColumn(0).Name,
			Status:           "waiting",
			TargetPK:         targetPKs[tableName],
			LastSuccessfulPK: 0,
		})
	}

	// ETA estimation
	// We do it here rather than in DataIteratorState to give the lock back
	// ASAP. It's not supposed to be that accurate anyway.
	var totalPKsToCopy uint64 = 0
	var completedPKs uint64 = 0
	estimatedPKsPerSecond := f.StateTracker.CopyStage.EstimatedPKsPerSecond()
	for _, targetPK := range targetPKs {
		totalPKsToCopy += targetPK
	}

	for _, completedPK := range lastSuccessfulPKs {
		completedPKs += completedPK
	}

	status.ETA = time.Duration(math.Ceil(float64(totalPKsToCopy-completedPKs)/estimatedPKsPerSecond)) * time.Second
	status.PKsPerSecond = uint64(estimatedPKsPerSecond)

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
