package ghostferry

import (
	"fmt"
	"sort"
	"time"

	"github.com/siddontang/go-mysql/mysql"
)

// NOTE: This file is only used for the ControlServer for now.
// TODO: eventually merge this into the ControlServer and use the Progress struct.

type TableStatus struct {
	TableName                   string
	PaginationKeyName           string
	Status                      string
	LastSuccessfulPaginationKey uint64
	TargetPaginationKey         uint64
}

type Status struct {
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
	TableStatuses       []*TableStatus
	AllTableNames       []string
	AllDatabaseNames    []string

	VerifierSupport     bool
	VerifierAvailable   bool
	VerifierMessage     string
	VerificationStarted bool
	VerificationDone    bool
	VerificationResult  VerificationResult
	VerificationErr     error
}

func FetchStatus(f *Ferry, v Verifier) *Status {
	progress := f.Progress()
	status := &Status{}

	status.GhostferryVersion = VersionString

	status.SourceHostPort = fmt.Sprintf("%s:%d", f.Source.Host, f.Source.Port)
	status.TargetHostPort = fmt.Sprintf("%s:%d", f.Target.Host, f.Target.Port)

	status.OverallState = fmt.Sprintf("%s", f.OverallState.Load())

	status.StartTime = f.StartTime
	status.CurrentTime = time.Now()
	status.TimeTaken = time.Duration(progress.TimeTaken)

	status.BinlogStreamerLag = time.Duration(progress.BinlogStreamerLag)
	status.LastSuccessfulBinlogPos = progress.LastSuccessfulBinlogPos
	status.TargetBinlogPos = progress.FinalBinlogPos

	status.AutomaticCutover = f.Config.AutomaticCutover
	status.BinlogStreamerStopRequested = f.BinlogStreamer.stopRequested

	status.Throttled = progress.Throttled

	// Getting all table statuses
	status.TableStatuses = make([]*TableStatus, 0, len(f.Tables))
	status.CompletedTableCount = 0
	status.TotalTableCount = len(progress.Tables)
	status.AllTableNames = make([]string, 0, len(progress.Tables))

	dbSet := make(map[string]bool)
	for name, tableProgress := range progress.Tables {
		status.TableStatuses = append(status.TableStatuses, &TableStatus{
			TableName:                   name,
			PaginationKeyName:           f.Tables[name].GetPaginationColumn().Name,
			Status:                      tableProgress.CurrentAction,
			LastSuccessfulPaginationKey: tableProgress.LastSuccessfulPaginationKey,
			TargetPaginationKey:         tableProgress.LastSuccessfulPaginationKey,
		})

		status.AllTableNames = append(status.AllTableNames, name)
		dbSet[f.Tables[name].Schema] = true

		if tableProgress.CurrentAction == TableActionCompleted {
			status.CompletedTableCount++
		}
	}

	status.AllDatabaseNames = make([]string, 0, len(dbSet))
	for dbName := range dbSet {
		status.AllDatabaseNames = append(status.AllDatabaseNames, dbName)
	}

	sort.Strings(status.AllDatabaseNames)
	sort.Strings(status.AllTableNames)

	// ETA estimation
	// We do it here rather than in DataIteratorState to give the lock back
	// ASAP. It's not supposed to be that accurate anyway.
	status.ETA = time.Duration(progress.ETA)
	status.PaginationKeysPerSecond = progress.PaginationKeysPerSecond

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
