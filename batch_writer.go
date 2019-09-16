package ghostferry

import (
	"database/sql"
	"fmt"

	"github.com/sirupsen/logrus"
)

type BatchWriterVerificationFailed struct {
	mismatchedPks []uint64
	table         string
}

func (e BatchWriterVerificationFailed) Error() string {
	return fmt.Sprintf("row fingerprints for pks %v on %v do not match", e.mismatchedPks, e.table)
}

type BatchWriter struct {
	DB             *sql.DB
	InlineVerifier *InlineVerifier
	StateTracker   *StateTracker

	DatabaseRewrites map[string]string
	TableRewrites    map[string]string

	WriteRetries int

	stmtCache *StmtCache
	logger    *logrus.Entry
}

func (w *BatchWriter) Initialize() {
	w.stmtCache = NewStmtCache()
	w.logger = logrus.WithField("tag", "batch_writer")
}

func (w *BatchWriter) WriteRowBatch(batch *RowBatch) error {
	return WithRetries(w.WriteRetries, 0, w.logger, "write batch to target", func() error {
		values := batch.Values()
		if len(values) == 0 {
			return nil
		}

		startPkpos, err := values[0].GetUint64(batch.PkIndex())
		if err != nil {
			return err
		}

		endPkpos, err := values[len(values)-1].GetUint64(batch.PkIndex())
		if err != nil {
			return err
		}

		db := batch.TableSchema().Schema
		if targetDbName, exists := w.DatabaseRewrites[db]; exists {
			db = targetDbName
		}

		table := batch.TableSchema().Name
		if targetTableName, exists := w.TableRewrites[table]; exists {
			table = targetTableName
		}

		query, args, err := batch.AsSQLQuery(db, table)
		if err != nil {
			return fmt.Errorf("during generating sql query at pk %v -> %v: %v", startPkpos, endPkpos, err)
		}

		stmt, err := w.stmtCache.StmtFor(w.DB, query)
		if err != nil {
			return fmt.Errorf("during prepare query near pk %v -> %v (%s): %v", startPkpos, endPkpos, query, err)
		}

		tx, err := w.DB.Begin()
		if err != nil {
			return fmt.Errorf("unable to begin transaction in BatchWriter: %v", err)
		}

		_, err = tx.Stmt(stmt).Exec(args...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("during exec query near pk %v -> %v (%s): %v", startPkpos, endPkpos, query, err)
		}

		if w.InlineVerifier != nil {
			mismatches, err := w.InlineVerifier.CheckFingerprintInline(tx, db, table, batch)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("during fingerprint checking for pk %v -> %v (%s): %v", startPkpos, endPkpos, query, err)
			}

			if len(mismatches) > 0 {
				tx.Rollback()
				return BatchWriterVerificationFailed{mismatches, batch.TableSchema().String()}
			}
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("during commit near pk %v -> %v (%s): %v", startPkpos, endPkpos, query, err)
		}

		// Note that the state tracker expects us the track based on the original
		// database and table names as opposed to the target ones.
		if w.StateTracker != nil {
			w.StateTracker.UpdateLastSuccessfulPK(batch.TableSchema().String(), endPkpos)
		}

		return nil
	})
}
