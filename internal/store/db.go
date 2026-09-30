// Package store owns the SQLite database: connections, transactions and
// migrations. It is the only package (together with the queue's SQL
// implementation) that knows SQL; the rest of the system sees interfaces.
package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// DB is a pair of connection pools on one SQLite file.
//
// SQLite allows a single writer at a time. Funnelling all writes through one
// connection avoids most SQLITE_BUSY errors and makes ordering easy to reason
// about; WAL mode lets the read pool run concurrently with that writer.
type DB struct {
	Write *sql.DB // exactly one connection
	Read  *sql.DB // several connections, query_only
}

// Open opens (creating if needed) the database at path and applies any
// pending migrations.
func Open(ctx context.Context, path string) (*DB, error) {
	w, err := sql.Open(driverName, dsn(path, true))
	if err != nil {
		return nil, fmt.Errorf("store: open write pool: %w", err)
	}
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	if err := w.PingContext(ctx); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("store: ping write pool: %w", err)
	}

	if err := migrate(ctx, w); err != nil {
		_ = w.Close()
		return nil, err
	}

	r, err := sql.Open(driverName, dsn(path, false))
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("store: open read pool: %w", err)
	}
	r.SetMaxOpenConns(4)
	if err := r.PingContext(ctx); err != nil {
		_ = w.Close()
		_ = r.Close()
		return nil, fmt.Errorf("store: ping read pool: %w", err)
	}
	return &DB{Write: w, Read: r}, nil
}

// Close closes both pools.
func (d *DB) Close() error {
	rerr := d.Read.Close()
	werr := d.Write.Close()
	if werr != nil {
		return werr
	}
	return rerr
}

// migrate applies embedded migrations/NNNN_name.sql files in filename order,
// each in its own transaction, recording them in schema_migrations.
func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at INTEGER NOT NULL
	)`); err != nil {
		return fmt.Errorf("store: create schema_migrations: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("store: read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		var exists int
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, name).Scan(&exists); err != nil {
			return fmt.Errorf("store: check migration %s: %w", name, err)
		}
		if exists > 0 {
			continue
		}
		body, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("store: read migration %s: %w", name, err)
		}
		err = InTx(ctx, db, func(ctx context.Context) error {
			tx, _ := TxFrom(ctx)
			if _, err := tx.ExecContext(ctx, string(body)); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx,
				`INSERT INTO schema_migrations (version, applied_at) VALUES (?, strftime('%s','now') * 1000)`, name)
			return err
		})
		if err != nil {
			return fmt.Errorf("store: apply migration %s: %w", name, err)
		}
	}
	return nil
}
