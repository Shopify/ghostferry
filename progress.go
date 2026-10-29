package ghostferry

import (
	"math"

	"github.com/go-mysql-org/go-mysql/mysql"
)

const (
	TableActionWaiting   = "waiting"
	TableActionCopying   = "copying"
	TableActionCompleted = "completed"
)

type TableProgress struct {
	LastSuccessfulPaginationKey PaginationKey
	TargetPaginationKey         PaginationKey
	CurrentAction               string // Possible values are defined via the constants TableAction*
	RowsWritten                 uint64
	BatchSize                   uint64
	BytesWritten                uint64
}

// estimatePaginationETA uses differences within each table, so negative keys
// and a table that has advanced past its original upper bound cannot underflow.
// Zero means no estimate when a rate or a negative table's starting point is unknown.
func estimatePaginationETA(targets map[string]PaginationKey, state *SerializableState, rate float64) float64 {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return 0
	}
	var remaining float64
	for table, target := range targets {
		if state.CompletedTables[table] {
			continue
		}
		position := float64(0)
		if checkpoint := state.LastSuccessfulPaginationKeys[table]; checkpoint != nil {
			position = checkpoint.NumericPosition()
		} else if target.NumericPosition() < 0 {
			return 0
		}
		remaining += math.Max(0, target.NumericPosition()-position)
	}
	eta := math.Ceil(remaining / rate)
	if math.IsNaN(eta) || math.IsInf(eta, 0) {
		return 0
	}
	return eta
}

type Progress struct {
	// Possible values are defined in ferry.go
	// Shows what the ferry is currently doing in one word.
	CurrentState string

	// The Payload field of the ProgressCallback config will be copied to here
	// verbatim.
	// Example usecase: you can be sending all the status to some aggregation
	// server and you want some sort of custom identification with this field.
	CustomPayload string

	Tables                         map[string]TableProgress
	LastSuccessfulBinlogPos        mysql.Position
	LastSuccessfulBinlogCoordinate BinlogCoordinate
	BinlogStreamerLag              float64 // This is the amount of seconds the binlog streamer is lagging by (seconds)
	BinlogWriterLag                float64 // This is the amount of seconds the binlog writer is lagging by (seconds)
	Throttled                      bool

	// if the TargetVerifier is enabled, we emit this lag, otherwise this number will be 0
	TargetBinlogStreamerLag float64

	// The number of data iterators currently active.
	ActiveDataIterators int

	// The behaviour of Ghostferry varies with respect to the VerifierType.
	// For example: a long cutover is OK if
	VerifierType string

	// The message that the verifier may emit for additional information
	VerifierMessage string

	// These are some variables that are only filled when CurrentState == done.
	FinalBinlogPos        mysql.Position
	FinalBinlogCoordinate BinlogCoordinate

	// A best estimate on the speed at which the copying is taking place. If
	// there are large gaps in the PaginationKey space, this probably will be inaccurate.
	PaginationKeysPerSecond uint64
	ETA                     float64 // seconds
	TimeTaken               float64 // seconds spent copying data; stops growing once the copy completes
}
