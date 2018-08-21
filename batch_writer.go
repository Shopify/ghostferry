package ghostferry

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type BatchWriter struct {
	DB *sql.DB

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

func (w *BatchWriter) WriteRowBatch(ctx context.Context, batch *RowBatch) error {
	return WithRetriesContext(ctx, w.WriteRetries, 0, w.logger, "write batch to target", func() error {
		if batch.Size() == 0 {
			return nil
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
			return fmt.Errorf("during generating sql query: %v", err)
		}

		stmt, err := w.stmtFor(query)
		if err != nil {
			return fmt.Errorf("during preparing query (%s): %v", query, err)
		}

		_, err = stmt.Exec(args...)
		if err != nil {
			return fmt.Errorf("during exec query (%s): %v", query, err)
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
