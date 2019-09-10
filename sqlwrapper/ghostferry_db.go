package sqlwrapper

import (
	"context"
	sqlorig "database/sql"
)

type DB struct {
	*sqlorig.DB
}

type Tx struct {
	*sqlorig.Tx
}

func Open(driverName, dataSourceName string) (*DB, error) {
	sqlDB, err := sqlorig.Open(driverName, dataSourceName)
	return &DB{sqlDB}, err
}

func (db DB) PrepareContext(ctx context.Context, query string) (*sqlorig.Stmt, error) {
	return db.DB.PrepareContext(ctx, query)
}

func (db DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sqlorig.Result, error) {
	return db.DB.ExecContext(ctx, query, args...)
}

func (db DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sqlorig.Rows, error) {
	return db.DB.QueryContext(ctx, query, args...)
}

func (db DB) Exec(query string, args ...interface{}) (sqlorig.Result, error) {
	return db.DB.Exec(query, args...)
}

func (db DB) Prepare(query string) (*sqlorig.Stmt, error) {
	return db.DB.Prepare(query)
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
	return &Tx{tx}, err
}

func (tx Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sqlorig.Result, error) {
	return tx.Tx.ExecContext(ctx, query, args...)
}

func (tx Tx) Exec(query string, args ...interface{}) (sqlorig.Result, error) {
	return tx.Tx.Exec(query, args...)
}

func (tx Tx) Prepare(query string) (*sqlorig.Stmt, error) {
	return tx.Tx.Prepare(query)
}

func (tx Tx) PrepareContext(ctx context.Context, query string) (*sqlorig.Stmt, error) {
	return tx.Tx.PrepareContext(ctx, query)
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
