from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional, Iterable, Any, Sequence

DEFAULT_DB_PATH = Path.cwd() / "scripter.db"

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS scripts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    command TEXT NOT NULL,
    working_dir TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    concurrency_policy TEXT NOT NULL DEFAULT 'allow'
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id INTEGER NOT NULL,
    status TEXT NOT NULL,              -- queued | running | success | failed
    started_at TEXT,
    finished_at TEXT,
    exit_code INTEGER,
    stdout TEXT,
    stderr TEXT,
    trigger TEXT,
    FOREIGN KEY (script_id) REFERENCES scripts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id INTEGER NOT NULL,
    interval_seconds INTEGER,
    cron TEXT,
    tz TEXT,
    last_run TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (script_id) REFERENCES scripts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS file_triggers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    recursive INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    FOREIGN KEY (script_id) REFERENCES scripts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS webhooks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    script_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (script_id) REFERENCES scripts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS locks (
    key TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS one_shots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id INTEGER NOT NULL,
    run_at_utc TEXT NOT NULL,
    tz TEXT,
    fired_at_utc TEXT,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY(script_id) REFERENCES scripts(id)
    );

CREATE INDEX IF NOT EXISTS idx_one_shots_due
    ON one_shots(fired_at_utc, run_at_utc);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    payload_json TEXT,
    created_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_topic_id ON events(topic, id);

CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    script_id INTEGER NOT NULL,
    created_at_utc TEXT NOT NULL,
    UNIQUE(topic, script_id),
    FOREIGN KEY(script_id) REFERENCES scripts(id)
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_topic ON subscriptions(topic);

CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    subscription_id INTEGER NOT NULL,
    claimed_at_utc TEXT,
    claimed_by TEXT,
    processed_at_utc TEXT,
    status TEXT,
    UNIQUE(event_id, subscription_id),
    FOREIGN KEY(event_id) REFERENCES events(id),
    FOREIGN KEY(subscription_id) REFERENCES subscriptions(id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_claim
    ON deliveries(claimed_at_utc, processed_at_utc);

CREATE INDEX IF NOT EXISTS idx_deliveries_event
    ON deliveries(event_id);

CREATE TABLE IF NOT EXISTS run_hooks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    on_script_id INTEGER NOT NULL,
    on_status TEXT NOT NULL,
    target_script_id INTEGER NOT NULL,
    created_at_utc TEXT NOT NULL,
    UNIQUE(on_script_id, on_status, target_script_id),
    FOREIGN KEY(on_script_id) REFERENCES scripts(id),
    FOREIGN KEY(target_script_id) REFERENCES scripts(id)
);

CREATE INDEX IF NOT EXISTS idx_run_hooks_on
    ON run_hooks(on_script_id, on_status);

CREATE TABLE IF NOT EXISTS pending_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger_id TEXT NOT NULL,
    script_id INTEGER NOT NULL,
    payload_json TEXT,
    created_at_utc TEXT NOT NULL,
    claimed_at_utc TEXT,
    claimed_by TEXT,
    processed_at_utc TEXT,
    FOREIGN KEY(script_id) REFERENCES scripts(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pending_waiting_queue_one
ON pending_events(script_id)
WHERE queue_tag='queue_one'
  AND processed_at_utc IS NULL
  AND claimed_at_utc IS NULL;

CREATE INDEX IF NOT EXISTS idx_pending_events_ready
    ON pending_events(processed_at_utc, claimed_at_utc, id);

CREATE TABLE IF NOT EXISTS daemon_hooks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event TEXT NOT NULL,
    script_id INTEGER NOT NULL,
    created_at_utc TEXT NOT NULL,
    UNIQUE(event, script_id),
    FOREIGN KEY(script_id) REFERENCES scripts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_daemon_hooks_event
    ON daemon_hooks(event);

CREATE TABLE IF NOT EXISTS signal_hooks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal TEXT NOT NULL,
    script_id INTEGER NOT NULL,
    created_at_utc TEXT NOT NULL,
    UNIQUE(signal, script_id),
    FOREIGN KEY(script_id) REFERENCES scripts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_signal_hooks_signal
    ON signal_hooks(signal);

CREATE TABLE IF NOT EXISTS app_triggers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id INTEGER NOT NULL,
    process_name TEXT NULL,
    on_event TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    UNIQUE(script_id, process_name, on_event),
    FOREIGN KEY(script_id) REFERENCES scripts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_app_triggers_lookup
    ON app_triggers(process_name, on_event);
    
CREATE TABLE IF NOT EXISTS daemon_lock (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    owner TEXT NOT NULL,
    pid INTEGER NOT NULL,
    acquired_at_utc TEXT NOT NULL
);
"""

class Database:
    def __init__(self, path: Optional[Path] = None):
        self.path = path or DEFAULT_DB_PATH
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.path, timeout=5.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            # Recommended for multi-connection/multi-process durability & less locking pain
            conn.execute("PRAGMA journal_mode = WAL;")
            self._conn = conn
        return self._conn

    def init(self) -> None:
        conn = self.connect()
        conn.executescript(SCHEMA)
        conn.commit()
        self.migrate()

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        conn = self.connect()
        cur = conn.execute(sql, tuple(params))
        conn.commit()
        return cur

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        conn = self.connect()
        cur = conn.execute(sql, tuple(params))
        return list(cur.fetchall())

    def execute_returning(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        conn = self.connect()
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        conn.commit()
        return rows

    def migrate(self) -> None:
        conn = self.connect()
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(runs)").fetchall()]
        if "trigger" not in cols:
            conn.execute("ALTER TABLE runs ADD COLUMN trigger TEXT")

        s_cols = [r["name"] for r in conn.execute("PRAGMA table_info(schedules)").fetchall()]
        if "cron" not in s_cols:
            conn.execute("ALTER TABLE schedules ADD COLUMN cron TEXT")
        if "tz" not in s_cols:
            conn.execute("ALTER TABLE schedules ADD COLUMN tz TEXT")

        p_cols = [r["name"] for r in conn.execute("PRAGMA table_info(pending_events)").fetchall()]
        if "queue_tag" not in p_cols:
            conn.execute("ALTER TABLE pending_events ADD COLUMN queue_tag TEXT")
        conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_pending_waiting_queue_one
        ON pending_events(script_id)
        WHERE queue_tag='queue_one'
        AND processed_at_utc IS NULL
        AND claimed_at_utc IS NULL;
        """)
        conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None