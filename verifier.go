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

type Verifier interface {
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

	// A call to check if the verifier has been started already. Should
	// return instantaneously.
	IsStarted() bool

	// Returns the start time of the verifier if it has been started, 0
	// otherwise.
	StartTime() time.Time

	// A call to check if the verifier is done. Should return
	// instantaneously.
	IsDone() bool

	// Returns the done time of the verifier if it has been started, 0
	// otherwise.
	DoneTime() time.Time

	// If the verification has been completed successfully (without errors) and
	// the data checks out to be "correct", return &VerificationResult{true, ""},
	// nil. Otherwise, return &VerificationResult{false, "message"}, nil.
	//
	// If the verification is "done" but experienced an error during the check,
	// return nil, yourErr.
	//
	// If the verification has not been started or not done,
	// return nil, nil
	VerificationResult() (*VerificationResult, error)
}

type ChecksumTableVerifier struct {
	Tables           []*schema.Table
	DatabaseRewrites map[string]string
	TableRewrites    map[string]string
	SourceDB         *sql.DB
	TargetDB         *sql.DB

	started   *AtomicBoolean
	startTime time.Time
	doneTime  time.Time

	verificationResult *VerificationResult
	verificationErr    error

	logger *logrus.Entry
	wg     *sync.WaitGroup
}

func (v *ChecksumTableVerifier) Verify() (*VerificationResult, error) {
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

		query := fmt.Sprintf("CHECKSUM TABLE %s EXTENDED", sourceTable)
		logWithTable := v.logger.WithFields(logrus.Fields{
			"sourceTable": sourceTable,
			"targetTable": targetTable,
		})
		logWithTable.Info("checking table")

		sourceRow := v.SourceDB.QueryRow(query)
		sourceChecksum, err := v.fetchChecksumValueFromRow(sourceRow)
		if err != nil {
			logWithTable.WithError(err).Error("failed to checksum table on the source")
			return nil, err
		}

		query = fmt.Sprintf("CHECKSUM TABLE %s EXTENDED", targetTable)
		targetRow := v.TargetDB.QueryRow(query)
		targetChecksum, err := v.fetchChecksumValueFromRow(targetRow)
		if err != nil {
			logWithTable.WithError(err).Error("failed to checksum table on the target")
			return nil, err
		}

		logFields := logrus.Fields{
			"sourceChecksum": sourceChecksum,
			"targetChecksum": targetChecksum,
		}

		if sourceChecksum == targetChecksum {
			logWithTable.WithFields(logFields).Info("tables on source and target verified to match")
		} else {
			logWithTable.WithFields(logFields).Error("tables on source and target DOES NOT MATCH")
			return &VerificationResult{false, fmt.Sprintf("data on table %s (%s) mismatched", sourceTable, targetTable)}, nil
		}
	}

	return &VerificationResult{true, ""}, nil
}

func (v *ChecksumTableVerifier) fetchChecksumValueFromRow(row *sql.Row) (int64, error) {
	var tablename string
	var checksum sql.NullInt64

	err := row.Scan(&tablename, &checksum)
	if err != nil {
		return int64(0), err
	}

	if !checksum.Valid {
		return int64(0), errors.New("cannot find table")
	}

	return checksum.Int64, nil
}

func (v *ChecksumTableVerifier) StartInBackground() error {
	if v.SourceDB == nil || v.TargetDB == nil {
		return errors.New("must specify source and target db")
	}

	if v.IsStarted() && !v.IsDone() {
		return errors.New("verification is on going")
	}

	v.started = new(AtomicBoolean)
	v.started.Set(true)

	// Initialize/reset all variables
	v.startTime = time.Now()
	v.doneTime = time.Time{}
	v.verificationResult = nil
	v.verificationErr = nil
	v.logger = logrus.WithField("tag", "checksum_verifier")
	v.wg = &sync.WaitGroup{}

	v.logger.Info("checksum table verification started")

	v.wg.Add(1)
	go func() {
		defer func() {
			v.doneTime = time.Now()
			v.wg.Done()
		}()

		v.verificationResult, v.verificationErr = v.Verify()
	}()

	return nil
}

func (v *ChecksumTableVerifier) Wait() {
	v.wg.Wait()
}

func (v *ChecksumTableVerifier) IsStarted() bool {
	return v.started != nil && v.started.Get()
}

func (v *ChecksumTableVerifier) StartTime() time.Time {
	return v.startTime
}

func (v *ChecksumTableVerifier) IsDone() bool {
	return !v.doneTime.IsZero()
}

func (v *ChecksumTableVerifier) DoneTime() time.Time {
	return v.doneTime
}

func (v *ChecksumTableVerifier) VerificationResult() (*VerificationResult, error) {
	return v.verificationResult, v.verificationErr
}
