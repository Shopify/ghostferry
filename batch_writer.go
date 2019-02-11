package ghostferry

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type BatchWriter struct {
	DB           *sql.DB
	StateTracker *CopyStateTracker

	DatabaseRewrites map[string]string
	TableRewrites    map[string]string

	WriteRetries int

	mut        sync.RWMutex
	statements map[string]*sql.Stmt
	logger     *logrus.Entry
}

func (w *BatchWriter) Initialize() {
	w.statements = make(map[string]*sql.Stmt)
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

		query, args, err := batch.AsSQLQuery(&schema.Table{Schema: db, Name: table})
		if err != nil {
			return fmt.Errorf("during generating sql query at pk %v -> %v: %v", startPkpos, endPkpos, err)
		}

		stmt, err := w.stmtFor(query)
		if err != nil {
			return fmt.Errorf("during preparing query near pk %v -> %v (%s): %v", startPkpos, endPkpos, query, err)
		}

		_, err = stmt.Exec(args...)
		if err != nil {
			return fmt.Errorf("during exec query near pk %v -> %v (%s): %v", startPkpos, endPkpos, query, err)
		}

		// Note that the state tracker expects us the track based on the original
		// database and table names as opposed to the target ones.
		if w.StateTracker != nil {
			w.StateTracker.UpdateLastSuccessfulPK(batch.TableSchema().String(), endPkpos)
		}

		return nil
	})
}

func (w *BatchWriter) stmtFor(query string) (*sql.Stmt, error) {
	stmt, exists := w.getStmt(query)
	if !exists {
		return w.newStmtFor(query)
	}
	return stmt, nil
}

func (w *BatchWriter) newStmtFor(query string) (*sql.Stmt, error) {
	stmt, err := w.DB.Prepare(query)
	if err != nil {
		return nil, err
	}

	w.storeStmt(query, stmt)
	return stmt, nil
}

func (w *BatchWriter) storeStmt(query string, stmt *sql.Stmt) {
	w.mut.Lock()
	defer w.mut.Unlock()
	w.statements[query] = stmt
}

func (w *BatchWriter) getStmt(query string) (*sql.Stmt, bool) {
	w.mut.RLock()
	defer w.mut.RUnlock()
	stmt, exists := w.statements[query]
	return stmt, exists
}
