package ghostferry

import (
	"github.com/siddontang/go-mysql/mysql"
)

const (
	TableActionWaiting   = "waiting"
	TableActionCopying   = "copying"
	TableActionCompleted = "completed"
)

type TableProgress struct {
	TargetPaginationKey uint64
	CurrentAction       string // Possible values are defined via the constants TableAction*
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
	BinlogStreamerLag       float64 // seconds
	Throttled               bool

	// The behaviour of Ghostferry varies with respect to the VerifierType.
	// For example: a long cutover is OK if
	VerifierType string

	// The message that the verifier may emit for additional information
	VerifierMessage string

	// These are some variables that are only filled when CurrentState == done.
	FinalBinlogPos mysql.Position
	TimeTaken      float64 // seconds
}
