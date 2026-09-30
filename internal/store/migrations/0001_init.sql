-- 0001_init.sql
--
-- Only the jobs table is defined so far. The events, deliveries and attempts
-- tables are appended here (or in 0002_*.sql) when the engine is written.
--
-- All timestamps are unix milliseconds.

CREATE TABLE jobs (
                      id               TEXT    PRIMARY KEY,                  -- ULID: sortable, so id order ~ creation order
                      kind             TEXT    NOT NULL,                     -- 'route' | 'deliver' | ...
                      payload          BLOB    NOT NULL,                     -- small; carries IDs, not bodies
                      dedupe_key       TEXT,                                 -- optional; unique per kind
                      state            TEXT    NOT NULL DEFAULT 'ready'
                          CHECK (state IN ('ready', 'leased', 'done', 'dead')),
                      attempts         INTEGER NOT NULL DEFAULT 0,           -- incremented when leased, not when failed
                      max_attempts     INTEGER NOT NULL CHECK (max_attempts > 0),
                      run_at           INTEGER NOT NULL,                     -- eligible to run from this time
                      lease_owner      TEXT,                                 -- worker id; debugging only
                      lease_token      INTEGER NOT NULL DEFAULT 0,           -- fencing token: +1 on every lease
                      lease_expires_at INTEGER,
                      last_error       TEXT,
                      created_at       INTEGER NOT NULL,
                      updated_at       INTEGER NOT NULL,
                      finished_at      INTEGER                               -- set when the job becomes done or dead
) STRICT;

-- Dequeue scans only jobs that could actually run.
CREATE INDEX jobs_ready  ON jobs (run_at, id)       WHERE state = 'ready';
-- The reaper scans only jobs currently holding a lease.
CREATE INDEX jobs_leased ON jobs (lease_expires_at) WHERE state = 'leased';
-- Retention purges and dead-letter listing.
CREATE INDEX jobs_done   ON jobs (finished_at)      WHERE state = 'done';
CREATE INDEX jobs_dead   ON jobs (finished_at)      WHERE state = 'dead';
-- Idempotent enqueue.
CREATE UNIQUE INDEX jobs_dedupe ON jobs (kind, dedupe_key) WHERE dedupe_key IS NOT NULL;