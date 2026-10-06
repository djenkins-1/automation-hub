-- 0002_events_deliveries.sql
--
-- events:            every accepted event, kept for audit and so the router can load it
-- deliveries:        one row per (event, rule, action): "send this message to this sink"
-- delivery_attempts: one row per try at a delivery, for the audit trail
--
-- All timestamps are unix milliseconds.

CREATE TABLE events (
                        id              TEXT    PRIMARY KEY,                   -- ULID
                        source          TEXT    NOT NULL,
                        type            TEXT    NOT NULL,
                        subject         TEXT    NOT NULL DEFAULT '',
                        occurred_at     INTEGER NOT NULL,                      -- when it happened (producer's clock)
                        data            TEXT    NOT NULL,                      -- JSON object
                        idempotency_key TEXT,                                  -- optional; unique per source
                        received_at     INTEGER NOT NULL                       -- when the hub accepted it
) STRICT;

CREATE UNIQUE INDEX events_idempotency ON events (source, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX events_received ON events (received_at);

CREATE TABLE deliveries (
                            id              TEXT    PRIMARY KEY,                   -- ULID
                            event_id        TEXT    NOT NULL REFERENCES events (id) ON DELETE CASCADE,
                            rule            TEXT    NOT NULL,
                            action_index    INTEGER NOT NULL,
                            sink            TEXT    NOT NULL,
                            title           TEXT    NOT NULL,                      -- rendered at routing time
                            body            TEXT    NOT NULL,
                            idempotency_key TEXT    NOT NULL UNIQUE,               -- <event id>:<rule>:<action index>
                            status          TEXT    NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'delivered', 'failed')),
                            attempts        INTEGER NOT NULL DEFAULT 0,
                            last_error      TEXT,
                            created_at      INTEGER NOT NULL,
                            updated_at      INTEGER NOT NULL,
                            delivered_at    INTEGER
) STRICT;

CREATE INDEX deliveries_event  ON deliveries (event_id);
CREATE INDEX deliveries_status ON deliveries (status, id);

CREATE TABLE delivery_attempts (
                                   id          INTEGER PRIMARY KEY,
                                   delivery_id TEXT    NOT NULL REFERENCES deliveries (id) ON DELETE CASCADE,
                                   number      INTEGER NOT NULL,                          -- the queue's attempt number
                                   started_at  INTEGER NOT NULL,
                                   finished_at INTEGER NOT NULL,
                                   outcome     TEXT    NOT NULL CHECK (outcome IN ('delivered', 'retry', 'failed')),
                                   error       TEXT,
                                   detail      TEXT
) STRICT;

CREATE INDEX delivery_attempts_delivery ON delivery_attempts (delivery_id, id);