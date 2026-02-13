from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional, Iterable, Any

DEFAULT_DB_PATH = Path.cwd() / "scripter.db"

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS scripts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    command TEXT NOT NULL,
    working_dir TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS RUNS (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id INTEGER NOT NULL,
    status TEXT NOT NULL,              -- queued | running | success | failed
    started_at TEXT,
    finished_at TEXT,
    exit_code INTEGER,
    stdout TEXT,
    stderr TEXT,
    FOREIGN KEY (script_id) REFERENCES scripts(id) ON DELETE CASCADE
);
"""

class Database:
    def __init__(self, path: Optional[Path] = None):
        self.path = path or DEFAULT_DB_PATH
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(self.path)
            self._conn.row_factory = sqlite3.Row
        return self._conn
    
    def init(self) -> None:
        conn = self.connect()
        conn.executescript(SCHEMA)
        conn.commit()

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        conn = self.connect()
        cur = conn.execute(sql, tuple(params))
        conn.commit()
        return cur

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        conn = self.connect()
        cur = conn.execute(sql, tuple(params))
        return list(cur.fetchall())

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None