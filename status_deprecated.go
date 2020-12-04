package ghostferry

import (
	"fmt"
	"sort"
	"time"

	"github.com/siddontang/go-mysql/mysql"
)

// NOTE: This file is only used for the ControlServer for now.
// TODO: eventually merge this into the ControlServer and use the Progress struct.

type TableStatusDeprecated struct {
	TableName           string
	PaginationKeyName   string
	Status              string
	CompletedPercentage uint64
}

type StatusDeprecated struct {
	GhostferryVersion string

	SourceHostPort string
	TargetHostPort string

	OverallState      string
	StartTime         time.Time
	CurrentTime       time.Time
	TimeTaken         time.Duration
	BinlogStreamerLag time.Duration

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

	DataIteratorSpeed uint64
	DataIteratorETA   time.Duration

	VerifierSupport     bool
	VerifierAvailable   bool
	VerifierMessage     string
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

	status.OverallState = fmt.Sprintf("%s", f.OverallState.Load())
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
	status.TargetBinlogPos = f.BinlogStreamer.stopAtBinlogPosition

	status.Throttled = f.Throttler.Throttled()

	// Getting all table statuses
	status.TableStatuses = make([]*TableStatusDeprecated, 0, len(f.Tables))

	serializedState := f.StateTracker.Serialize(nil, nil)

	completedTables := serializedState.CompletedTables

	status.CompletedTableCount = len(completedTables)
	status.TotalTableCount = len(f.Tables)

	status.DataIteratorSpeed = uint64(serializedState.estimatedPaginationKeysPerSecond)

	if status.DataIteratorSpeed > 0 {
		status.DataIteratorETA = time.Duration(serializedState.CalculateKeysWaitingForCopy()/status.DataIteratorSpeed) * time.Second
	}

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

	for _, tableName := range status.AllTableNames {
		if _, ok := completedTables[tableName]; ok {
			completedTableNames = append(completedTableNames, tableName)
			continue
		}

		if _, ok := serializedState.BatchProgress[tableName]; ok {
			copyingTableNames = append(copyingTableNames, tableName)
			continue
		}

		waitingTableNames = append(waitingTableNames, tableName)
	}

	sort.Strings(completedTableNames)
	sort.Strings(copyingTableNames)
	sort.Strings(waitingTableNames)

	for _, tableName := range completedTableNames {
		status.TableStatuses = append(status.TableStatuses, &TableStatusDeprecated{
			TableName:           tableName,
			PaginationKeyName:   f.Tables[tableName].GetPaginationColumn().Name,
			Status:              "complete",
			CompletedPercentage: 100,
		})
	}

	for _, tableName := range copyingTableNames {
		status.TableStatuses = append(status.TableStatuses, &TableStatusDeprecated{
			TableName:           tableName,
			PaginationKeyName:   f.Tables[tableName].GetPaginationColumn().Name,
			Status:              "copying",
			CompletedPercentage: serializedState.CalculateTableProgress(tableName),
		})
	}

	for _, tableName := range waitingTableNames {
		status.TableStatuses = append(status.TableStatuses, &TableStatusDeprecated{
			TableName:           tableName,
			PaginationKeyName:   f.Tables[tableName].GetPaginationColumn().Name,
			Status:              "waiting",
			CompletedPercentage: 0,
		})
	}

	// Verifier display
	if v != nil {
		status.VerifierSupport = true

		result, err := v.Result()
		status.VerificationStarted = result.IsStarted()
		status.VerificationDone = result.IsDone()
		status.VerifierMessage = v.Message()

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
