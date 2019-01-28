package ghostferry

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type IncompleteVerificationError struct{}

func (e IncompleteVerificationError) Error() string {
	return "verification is incomplete"
}

type VerificationResult struct {
	DataCorrect bool
	Message     string
}

func (e VerificationResult) Error() string {
	return e.Message
}

type VerificationResultAndStatus struct {
	VerificationResult

	StartTime time.Time
	DoneTime  time.Time
}

func (r VerificationResultAndStatus) IsStarted() bool {
	return !r.StartTime.IsZero()
}

func (r VerificationResultAndStatus) IsDone() bool {
	return !r.DoneTime.IsZero()
}

// The sole purpose of this interface is to make it easier for one to
// implement their own strategy for verification and hook it up with
// the ControlServer. If there is no such need, one does not need to
// implement this interface.
type Verifier interface {
	// If the Verifier needs to do anything immediately after the DataIterator
	// finishes copying data and before cutover occurs, implement this function.
	VerifyBeforeCutover() error

	// The Ferry will use this method to tell the verifier what to check.
	//
	// TODO: this will be removed once we refactor how the TableSchemaCache is
	// handled.
	SetApplicableTableSchemaCache(TableSchemaCache)

	// Start the verifier in the background during the cutover phase.
	// Traditionally, this is called from within the ControlServer.
	//
	// This method maybe called multiple times and it's up to the verifier
	// to decide if it is possible to re-run the verification.
	StartInBackground() error

	// Wait for the verifier until it finishes verification after it was
	// started with the StartInBackground.
	//
	// A verification is "done" when it verified the dbs (either
	// correct or incorrect) OR when it experiences an error.
	Wait()

	// Returns the result and the status of the verification.
	// To check the status, call IsStarted() and IsDone() on
	// VerificationResultAndStatus.
	//
	// If the verification has been completed successfully (without errors) and
	// the data checks out to be "correct", the result will be
	// VerificationResult{true, ""}, with error = nil.
	// Otherwise, the result will be VerificationResult{false, "message"}, with
	// error = nil.
	//
	// If the verification is "done" but experienced an error during the check,
	// the result will be VerificationResult{}, with err = yourErr.
	Result() (VerificationResultAndStatus, error)
}

type ChecksumTableVerifier struct {
	Tables           []*schema.Table
	DatabaseRewrites map[string]string
	TableRewrites    map[string]string
	SourceDB         *sql.DB
	TargetDB         *sql.DB

	started *AtomicBoolean

	verificationResultAndStatus VerificationResultAndStatus
	verificationErr             error

	logger *logrus.Entry
	wg     *sync.WaitGroup
}

func (v *ChecksumTableVerifier) VerifyBeforeCutover() error {
	// All verification occurs in cutover for this verifier.
	return nil
}

func (v *ChecksumTableVerifier) SetApplicableTableSchemaCache(t TableSchemaCache) {
	v.Tables = t.AsSlice()
}

func (v *ChecksumTableVerifier) Verify() (VerificationResult, error) {
	if v.logger == nil {
		v.logger = logrus.WithField("tag", "checksum_verifier")
	}

	for _, table := range v.Tables {
		sourceTable := QuotedTableName(table)

		targetDbName := table.Schema
		if v.DatabaseRewrites != nil {
			if rewrittenName, exists := v.DatabaseRewrites[table.Schema]; exists {
				targetDbName = rewrittenName
			}
		}

		targetTableName := table.Name
		if v.TableRewrites != nil {
			if rewrittenName, exists := v.TableRewrites[table.Name]; exists {
				targetTableName = rewrittenName
			}
		}

		targetTable := QuotedTableNameFromString(targetDbName, targetTableName)

		logWithTable := v.logger.WithFields(logrus.Fields{
			"sourceTable": sourceTable,
			"targetTable": targetTable,
		})
		logWithTable.Info("checking table")

		wg := sync.WaitGroup{}
		var sourceChecksum, targetChecksum int64
		var sourceErr, targetErr error

		wg.Add(2)
		go func() {
			defer wg.Done()
			query := fmt.Sprintf("CHECKSUM TABLE %s EXTENDED", sourceTable)
			sourceRow := v.SourceDB.QueryRow(query)
			sourceChecksum, sourceErr = v.fetchChecksumValueFromRow(sourceRow)
		}()

		go func() {
			defer wg.Done()
			query := fmt.Sprintf("CHECKSUM TABLE %s EXTENDED", targetTable)
			targetRow := v.TargetDB.QueryRow(query)
			targetChecksum, targetErr = v.fetchChecksumValueFromRow(targetRow)
		}()
		wg.Wait()

		if sourceErr != nil {
			logWithTable.WithError(sourceErr).Error("failed to checksum table on the source")
			return VerificationResult{}, sourceErr
		}

		if targetErr != nil {
			logWithTable.WithError(targetErr).Error("failed to checksum table on the target")
			return VerificationResult{}, targetErr
		}

		logFields := logrus.Fields{
			"sourceChecksum": sourceChecksum,
			"targetChecksum": targetChecksum,
		}

		if sourceChecksum == targetChecksum {
			logWithTable.WithFields(logFields).Info("tables on source and target verified to match")
		} else {
			logWithTable.WithFields(logFields).Error("tables on source and target DOES NOT MATCH")
			return VerificationResult{false, fmt.Sprintf("data on table %s (%s) mismatched", sourceTable, targetTable)}, nil
		}
	}

	return VerificationResult{true, ""}, nil
}

func (v *ChecksumTableVerifier) fetchChecksumValueFromRow(row *sql.Row) (int64, error) {
	var tablename string
	var checksum sql.NullInt64

	err := row.Scan(&tablename, &checksum)
	if err != nil {
		return int64(0), err
	}

	if !checksum.Valid {
		return int64(0), fmt.Errorf("cannot find table %s during verification", tablename)
	}

	return checksum.Int64, nil
}

func (v *ChecksumTableVerifier) StartInBackground() error {
	if v.SourceDB == nil || v.TargetDB == nil {
		return errors.New("must specify source and target db")
	}

	if v.started != nil && v.started.Get() && !v.verificationResultAndStatus.IsDone() {
		return errors.New("verification is on going")
	}

	v.started = new(AtomicBoolean)
	v.started.Set(true)

	// Initialize/reset all variables
	v.verificationResultAndStatus = VerificationResultAndStatus{
		StartTime: time.Now(),
		DoneTime:  time.Time{},
	}
	v.verificationErr = nil
	v.logger = logrus.WithField("tag", "checksum_verifier")
	v.wg = &sync.WaitGroup{}

	v.logger.Info("checksum table verification started")

	v.wg.Add(1)
	go func() {
		defer v.wg.Done()

		v.verificationResultAndStatus.VerificationResult, v.verificationErr = v.Verify()
		v.verificationResultAndStatus.DoneTime = time.Now()
		v.started.Set(false)
	}()

	return nil
}

func (v *ChecksumTableVerifier) Wait() {
	v.wg.Wait()
}

func (v *ChecksumTableVerifier) Result() (VerificationResultAndStatus, error) {
	return v.verificationResultAndStatus, v.verificationErr
}
