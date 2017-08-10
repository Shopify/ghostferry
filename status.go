package ghostferry

import (
	"fmt"
	"sort"
	"time"

	"github.com/siddontang/go-mysql/mysql"
)

func sortedSetKeys(m map[string]bool) []string {
	ks := make([]string, len(m))
	i := 0
	for k, _ := range m {
		ks[i] = k
		i++
	}

	sort.Strings(ks)

	return ks
}

type TableStatus struct {
	TableName        string
	PrimaryKeyName   string
	Status           string
	LastSuccessfulPK int64
	TargetPK         int64
}

type Status struct {
	SourceHostPort      string
	TargetHostPort      string
	ApplicableDatabases []string
	ApplicableTables    []string

	OverallState string
	StartTime    time.Time
	CurrentTime  time.Time
	TimeTaken    time.Duration
	ETA          time.Duration

	AutomaticCutover            bool
	BinlogStreamerStopRequested bool
	LastSuccessfulBinlogPos     mysql.Position
	TargetBinlogPos             mysql.Position
	BinlogStreamerLag           time.Duration

	Throttled      bool
	ThrottledUntil time.Time

	CompletedTableCount int
	TotalTableCount     int
	TableStatuses       []*TableStatus

	VerifierAvailable   bool
	VerificationStarted bool
	VerificationDone    bool
	MismatchedTables    []string
	VerificationErr     error
}

func FetchStatus(f *Ferry) *Status {
	status := &Status{}

	status.SourceHostPort = fmt.Sprintf("%s:%d", f.SourceHost, f.SourcePort)
	status.TargetHostPort = fmt.Sprintf("%s:%d", f.TargetHost, f.TargetPort)
	status.ApplicableDatabases = sortedSetKeys(f.ApplicableDatabases)
	status.ApplicableTables = sortedSetKeys(f.ApplicableTables)

	status.OverallState = f.OverallState
	status.StartTime = f.StartTime
	status.CurrentTime = time.Now()
	if f.DoneTime.IsZero() {
		status.TimeTaken = status.CurrentTime.Sub(status.StartTime)
	} else {
		status.TimeTaken = f.DoneTime.Sub(status.StartTime)
	}
	// TODO: ETA estimation

	status.AutomaticCutover = f.Config.AutomaticCutover
	status.BinlogStreamerStopRequested = f.BinlogStreamer.stopRequested
	status.LastSuccessfulBinlogPos = f.BinlogStreamer.lastStreamedBinlogPosition
	status.TargetBinlogPos = f.BinlogStreamer.targetBinlogPosition
	// TODO: Binlog Streamer Lag

	status.ThrottledUntil = f.Throttler.ThrottleUntil()
	status.Throttled = status.ThrottledUntil.After(status.CurrentTime)

	// Getting all table statuses
	status.TableStatuses = make([]*TableStatus, 0, len(f.Tables))
	completedTables := f.DataIterator.CurrentState.CompletedTables()
	targetPKs := f.DataIterator.CurrentState.TargetPrimaryKeys()
	lastSuccessfulPKs := f.DataIterator.CurrentState.LastSuccessfulPrimaryKeys()

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
		if _, ok := lastSuccessfulPKs[tableName]; ok {
			continue // already started, therefore not waiting
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
			TargetPK:         -1,
			LastSuccessfulPK: -1,
		})
	}

	if f.Verifier != nil {
		status.VerificationStarted = f.Verifier.VerificationStarted()
		status.VerificationDone = f.Verifier.VerificationDone()
		status.VerifierAvailable = !status.VerificationStarted || status.VerificationDone
		status.MismatchedTables, status.VerificationErr = f.Verifier.MismatchedTables()
	} else {
		status.VerifierAvailable = false
	}

	return status
}
