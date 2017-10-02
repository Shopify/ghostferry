package ghostferry

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type Verifier interface {
	StartVerification(*Ferry)
	VerificationStarted() bool
	VerificationDone() bool
	VerifiedCorrect() (bool, error)
	MismatchedTables() ([]string, error)
	Wait()
}

type ChecksumTableVerifier struct {
	*sync.WaitGroup

	StartTime time.Time
	DoneTime  time.Time

	TablesToCheck []*schema.Table

	mismatchedTables []string
	err              error

	logger *logrus.Entry
}

func (this *ChecksumTableVerifier) StartVerification(f *Ferry) {
	if this.VerificationStarted() && !this.VerificationDone() {
		this.logger.Warn("cannot start another verification attempt if one is running, ignoring...")
		return
	}

	this.WaitGroup = &sync.WaitGroup{}
	this.logger = logrus.WithField("tag", "checksum_verifier")
	this.Add(1)
	go func() {
		defer this.Done()
		this.Run(f)
	}()
}

func (this *ChecksumTableVerifier) Run(f *Ferry) {
	this.StartTime = time.Now()
	defer func() {
		this.DoneTime = time.Now()
	}()

	this.DoneTime = time.Time{}
	this.mismatchedTables = make([]string, 0)
	this.err = nil

	sourceTableChecksums := make(map[string]int64)

	// Quote the tables
	tablesToCheck := make([]string, len(this.TablesToCheck))
	for i, table := range this.TablesToCheck {
		tablesToCheck[i] = QuotedTableName(table)
	}

	query := fmt.Sprintf("CHECKSUM TABLE %s EXTENDED", strings.Join(tablesToCheck, ", "))
	this.logger.WithField("sql", query).Info("performing checksum tables...")

	sourceRows, err := f.SourceDB.Query(query)
	if err != nil {
		this.logger.WithError(err).Error("failed to checksum source tables")
		this.err = err
		return
	}

	defer sourceRows.Close()

	for sourceRows.Next() {
		var tablename string
		var checksum sql.NullInt64

		err = sourceRows.Scan(&tablename, &checksum)
		if err != nil {
			this.logger.WithError(err).Error("failed to scan row during source checksum tables")
			this.err = err
			return
		}

		if !checksum.Valid {
			err = fmt.Errorf("cannot find table %s on the source database", tablename)
			this.logger.WithError(err).Error("cannot find table on source database")
			this.err = err
			return
		}

		sourceTableChecksums[tablename] = checksum.Int64
	}

	targetRows, err := f.TargetDB.Query(query)
	if err != nil {
		this.logger.WithError(err).Error("failed to checksum target tables")
		this.err = err
		return
	}
	defer targetRows.Close()

	for targetRows.Next() {
		var tablename string
		var checksum sql.NullInt64

		err = targetRows.Scan(&tablename, &checksum)
		if err != nil {
			this.logger.WithError(err).Error("failed to scan rows during target checksum tables")
			this.err = err
			return
		}

		checksumFields := logrus.Fields{
			"source_checksum": sourceTableChecksums[tablename],
			"target_checksum": checksum.Int64,
			"target_exists":   checksum.Valid,
			"table":           tablename,
		}

		if checksum.Valid && checksum.Int64 == sourceTableChecksums[tablename] {
			this.logger.WithFields(checksumFields).Info("tables verified to match")
		} else {
			this.logger.WithFields(checksumFields).Error("table verification failed")
			this.mismatchedTables = append(this.mismatchedTables, tablename)
		}
	}

}

func (this *ChecksumTableVerifier) VerificationStarted() bool {
	return !this.StartTime.IsZero()
}

func (this *ChecksumTableVerifier) VerificationDone() bool {
	return !this.DoneTime.IsZero()
}

func (this *ChecksumTableVerifier) VerifiedCorrect() (bool, error) {
	return len(this.mismatchedTables) == 0, this.err
}

func (this *ChecksumTableVerifier) MismatchedTables() ([]string, error) {
	return this.mismatchedTables, this.err
}
