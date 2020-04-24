package sqlwrapper

import (
	"context"
	sqlorig "database/sql"
	"fmt"
)

type DB struct {
	*sqlorig.DB
	Marginalia string
}

type Tx struct {
	*sqlorig.Tx
	marginalia string
}

func Open(driverName, dataSourceName, marginalia string) (*DB, error) {
	sqlDB, err := sqlorig.Open(driverName, dataSourceName)
	return &DB{sqlDB, marginalia}, err
}

func (db DB) PrepareContext(ctx context.Context, query string) (*sqlorig.Stmt, error) {
	return db.DB.PrepareContext(ctx, AnnotateStmt(query, db.Marginalia))
}

func (db DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sqlorig.Result, error) {
	return db.DB.ExecContext(ctx, AnnotateStmt(query, db.Marginalia), args...)
}

func (db DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sqlorig.Rows, error) {
	return db.DB.QueryContext(ctx, query, args...)
}

func (db DB) Exec(query string, args ...interface{}) (sqlorig.Result, error) {
	return db.DB.Exec(AnnotateStmt(query, db.Marginalia), args...)
}

func (db DB) Prepare(query string) (*sqlorig.Stmt, error) {
	return db.DB.Prepare(AnnotateStmt(query, db.Marginalia))
}

func (db DB) Query(query string, args ...interface{}) (*sqlorig.Rows, error) {
	return db.DB.Query(query, args...)
}

func (db DB) QueryRow(query string, args ...interface{}) *sqlorig.Row {
	return db.DB.QueryRow(query, args...)
}

func (db DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sqlorig.Row {
	return db.DB.QueryRowContext(ctx, query, args...)
}

func (db DB) Begin() (*Tx, error) {
	tx, err := db.DB.Begin()
	return &Tx{tx, db.Marginalia}, err
}

func (tx Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sqlorig.Result, error) {
	return tx.Tx.ExecContext(ctx, AnnotateStmt(query, tx.marginalia), args...)
}

func (tx Tx) Exec(query string, args ...interface{}) (sqlorig.Result, error) {
	return tx.Tx.Exec(AnnotateStmt(query, tx.marginalia), args...)
}

func (tx Tx) Prepare(query string) (*sqlorig.Stmt, error) {
	return tx.Tx.Prepare(AnnotateStmt(query, tx.marginalia))
}

func (tx Tx) PrepareContext(ctx context.Context, query string) (*sqlorig.Stmt, error) {
	return tx.Tx.PrepareContext(ctx, AnnotateStmt(query, tx.marginalia))
}

func (tx Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sqlorig.Rows, error) {
	return tx.Tx.QueryContext(ctx, query, args)
}

func (tx Tx) Query(query string, args ...interface{}) (*sqlorig.Rows, error) {
	return tx.Tx.Query(query, args...)
}

func (tx Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sqlorig.Row {
	return tx.Tx.QueryRowContext(ctx, query, args...)
}

func (tx Tx) QueryRow(query string, args ...interface{}) *sqlorig.Row {
	return tx.Tx.QueryRow(query, args...)
}

// AnnotateStmt annotates a single SQL statement with the configured marginalia.
//
// *NOTE*
// This is NOT SAFE to use with multiple SQL statements as it naively annotates
// the single query string provided and does not attempt to parse the provided SQL
func AnnotateStmt(query, marginalia string) string {
	return fmt.Sprintf("/*%s*/ %s", marginalia, query)
}
