package store

// This file is the only place that knows which SQLite driver is used.
// modernc.org/sqlite is pure Go, so builds work with CGO_ENABLED=0 and the
// final Docker image can be distroless/static.

import (
	_ "modernc.org/sqlite"
)

const driverName = "sqlite"

// dsn builds a connection string for the given database file.
//
// The write pool opens transactions with BEGIN IMMEDIATE (_txlock=immediate),
// so lock contention surfaces when the transaction starts, not halfway through
// it. The read pool is marked query_only as a safety net.
func dsn(path string, write bool) string {
	const common = "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)"
	if write {
		return "file:" + path + "?" + common + "&_pragma=synchronous(NORMAL)&_txlock=immediate"
	}
	return "file:" + path + "?" + common + "&_pragma=query_only(ON)"
}
