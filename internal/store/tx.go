package store

import (
	"context"
	"database/sql"
	"fmt"
)

// DBTX is the subset of *sql.DB and *sql.Tx that repositories and the queue
// need. Code written against DBTX works both inside and outside a transaction.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type txKey struct{}

// WithTx returns a context carrying tx. Repositories that call Executor with
// this context join the transaction automatically.
func WithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// TxFrom returns the transaction carried by ctx, if any.
func TxFrom(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	return tx, ok
}

// Executor returns the transaction carried by ctx if there is one, otherwise
// fallback. This is what lets ingest write an event and enqueue a job in one
// atomic transaction even though the event store and the queue are separate
// packages that never import each other.
func Executor(ctx context.Context, fallback DBTX) DBTX {
	if tx, ok := TxFrom(ctx); ok {
		return tx
	}
	return fallback
}

// InTx runs fn inside a transaction on db. If ctx already carries a
// transaction, fn simply joins it (there are no nested transactions or
// savepoints). The transaction commits if fn returns nil and rolls back on
// error or panic.
//
// db should be the write pool: with _txlock=immediate the transaction takes
// the write lock up front.
func InTx(ctx context.Context, db *sql.DB, fn func(ctx context.Context) error) (err error) {
	if _, ok := TxFrom(ctx); ok {
		return fn(ctx)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(WithTx(ctx, tx)); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}
