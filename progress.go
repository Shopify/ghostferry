package ghostferry

import (
	"github.com/go-mysql-org/go-mysql/mysql"
)

const (
	TableActionWaiting   = "waiting"
	TableActionCopying   = "copying"
	TableActionCompleted = "completed"
)

type TableProgress struct {
	LastSuccessfulPaginationKey uint64
	TargetPaginationKey         uint64
	CurrentAction               string // Possible values are defined via the constants TableAction*
	RowsWritten                 uint64
	BatchSize                   uint64
	BytesWritten                uint64
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

	Tables                  map[string]TableProgress
	LastSuccessfulBinlogPos mysql.Position
	LastSuccessfulGTID      string
	BinlogStreamerLag       float64 // This is the amount of seconds the binlog streamer is lagging by (seconds)
	BinlogWriterLag         float64 // This is the amount of seconds the binlog writer is lagging by (seconds)
	Throttled               bool

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
	FinalBinlogPos mysql.Position

	// A best estimate on the speed at which the copying is taking place. If
	// there are large gaps in the PaginationKey space, this probably will be inaccurate.
	PaginationKeysPerSecond uint64
	ETA                     float64 // seconds
	TimeTaken               float64 // seconds
}
