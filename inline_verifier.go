package ghostferry

import (
	"database/sql"

	"github.com/sirupsen/logrus"
)

type InlineVerifier struct {
	SourceDB *sql.DB
	TargetDB *sql.DB

	sourceStmtCache *StmtCache
	targetStmtCache *StmtCache
	logger          *logrus.Entry
}

func (v *InlineVerifier) VerifyBeforeCutover() error {
	// TODO: Iterate until the reverify queue is small enough
	return nil
}

func (v *InlineVerifier) VerifyDuringCutover() (VerificationResult, error) {
	// TODO: verify everything within the reverify queue.
	return VerificationResult{}, nil
}

func (v *InlineVerifier) StartInBackground() error {
	// not needed?
	return nil
}

func (v *InlineVerifier) Wait() {
	// not needed?
}

func (v *InlineVerifier) Result() (VerificationResultAndStatus, error) {
	// not implemented for now
	return VerificationResultAndStatus{}, nil
}

func (v *InlineVerifier) CheckFingerprintInline(tx *sql.Tx, targetDb, targetTable string, sourceBatch *RowBatch) ([]uint64, error) {
	return nil, nil
}
