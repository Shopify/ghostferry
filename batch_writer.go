package ghostferry

import (
	"fmt"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/sirupsen/logrus"
)

type BatchWriterVerificationFailed struct {
	mismatchedPaginationKeys []uint64
	table                    string
}

func (e BatchWriterVerificationFailed) Error() string {
	return fmt.Sprintf("row fingerprints for paginationKeys %v on %v do not match", e.mismatchedPaginationKeys, e.table)
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
	return WithRetries(w.WriteRetries, 500*time.Millisecond, w.logger, "write batch to target", func() error {
		values := batch.Values()
		if len(values) == 0 {
			return nil
		}

		startPaginationKeypos, err := values[0].GetUint64(batch.PaginationKeyIndex())
		if err != nil {
			return err
		}

		endPaginationKeypos, err := values[len(values)-1].GetUint64(batch.PaginationKeyIndex())
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
			return fmt.Errorf("during generating sql query at paginationKey %v -> %v: %v", startPaginationKeypos, endPaginationKeypos, err)
		}

		stmt, err := w.stmtCache.StmtFor(w.DB, query)
		if err != nil {
			return fmt.Errorf("during prepare query near paginationKey %v -> %v (%s): %v", startPaginationKeypos, endPaginationKeypos, query, err)
		}

		tx, err := w.DB.Begin()
		if err != nil {
			return fmt.Errorf("unable to begin transaction in BatchWriter: %v", err)
		}

		_, err = tx.Stmt(stmt).Exec(args...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("during exec query near paginationKey %v -> %v (%s): %v", startPaginationKeypos, endPaginationKeypos, query, err)
		}

		if w.InlineVerifier != nil {
			mismatches, err := w.InlineVerifier.CheckFingerprintInline(tx, db, table, batch)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("during fingerprint checking for paginationKey %v -> %v (%s): %v", startPaginationKeypos, endPaginationKeypos, query, err)
			}

			if len(mismatches) > 0 {
				tx.Rollback()
				return BatchWriterVerificationFailed{mismatches, batch.TableSchema().String()}
			}
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("during commit near paginationKey %v -> %v (%s): %v", startPaginationKeypos, endPaginationKeypos, query, err)
		}

		// Note that the state tracker expects us the track based on the original
		// database and table names as opposed to the target ones.
		if w.StateTracker != nil {
			w.StateTracker.UpdateLastSuccessfulPaginationKey(batch.TableSchema().String(), endPaginationKeypos)
		}

		return nil
	})
}
