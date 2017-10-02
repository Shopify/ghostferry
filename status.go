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
	LastSuccessfulPK int64
	TargetPK         int64
}

type Status struct {
	GhostferryVersion string

	SourceHostPort      string
	TargetHostPort      string
	ApplicableDatabases map[string]bool
	ApplicableTables    map[string]bool

	OverallState      string
	StartTime         time.Time
	CurrentTime       time.Time
	TimeTaken         time.Duration
	ETA               time.Duration
	BinlogStreamerLag time.Duration
	PKsPerSecond      int64

	AutomaticCutover            bool
	BinlogStreamerStopRequested bool
	LastSuccessfulBinlogPos     mysql.Position
	TargetBinlogPos             mysql.Position

	Throttled      bool
	ThrottledUntil time.Time

	CompletedTableCount int
	TotalTableCount     int
	TableStatuses       []*TableStatus

	VerifierSupport     bool
	VerifierAvailable   bool
	VerificationStarted bool
	VerificationDone    bool
	MismatchedTables    []string
	VerificationErr     error
}

func FetchStatus(f *Ferry) *Status {
	status := &Status{}

	status.GhostferryVersion = fmt.Sprintf("%s+%s", VersionNumber, VersionCommit)

	status.SourceHostPort = fmt.Sprintf("%s:%d", f.SourceHost, f.SourcePort)
	status.TargetHostPort = fmt.Sprintf("%s:%d", f.TargetHost, f.TargetPort)
	status.ApplicableDatabases = f.ApplicableDatabases
	status.ApplicableTables = f.ApplicableTables

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

	status.ThrottledUntil = f.Throttler.ThrottleUntil()
	status.Throttled = status.ThrottledUntil.After(status.CurrentTime)

	// Getting all table statuses
	status.TableStatuses = make([]*TableStatus, 0, len(f.Tables))
	completedTables := f.DataIterator.CurrentState.CompletedTables()
	targetPKs := f.DataIterator.CurrentState.TargetPrimaryKeys()
	lastSuccessfulPKs := f.DataIterator.CurrentState.LastSuccessfulPrimaryKeys()

	status.CompletedTableCount = len(completedTables)
	status.TotalTableCount = len(f.Tables)

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
	var totalPKsToCopy int64 = 0
	var completedPKs int64 = 0
	estimatedPKsPerSecond := f.DataIterator.CurrentState.EstimatedPKProcessedPerSecond()
	for _, targetPK := range targetPKs {
		totalPKsToCopy += targetPK
	}

	for _, completedPK := range lastSuccessfulPKs {
		completedPKs += completedPK
	}

	status.ETA = time.Duration(math.Ceil(float64(totalPKsToCopy-completedPKs)/estimatedPKsPerSecond)) * time.Second
	status.PKsPerSecond = int64(estimatedPKsPerSecond)

	// Verifier display
	if f.Verifier != nil {
		status.VerifierSupport = true
		status.VerificationStarted = f.Verifier.VerificationStarted()
		status.VerificationDone = f.Verifier.VerificationDone()

		// We can only run the verifier if we're not copying and not verifying
		status.VerifierAvailable = status.OverallState != StateStarting && status.OverallState != StateCopying && (!status.VerificationStarted || status.VerificationDone)
		status.MismatchedTables, status.VerificationErr = f.Verifier.MismatchedTables()
	} else {
		status.VerifierSupport = false
		status.VerifierAvailable = false
	}

	return status
}