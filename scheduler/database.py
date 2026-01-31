from __future__ import annotations

import sqlite3
import json as _json
import uuid
import threading
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional, List

from .models import Script

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scripts (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    path TEXT NOT NULL,
    hash TEXT,
    enabled INTEGER DEFAULT 1,
    created_at TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS schedules (
    id TEXT PRIMARY KEY,
    script_id TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    trigger_config TEXT NOT NULL,
    FOREIGN KEY (script_id) REFERENCES scripts(id)
);

CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY,
    script_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT,
    exit_code INTEGER,
    stdout TEXT,
    stderr TEXT,
    FOREIGN KEY (script_id) REFERENCES scripts(id)
);
"""

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self._lock = threading.Lock()
    
    def connect(self) -> None:
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
    
    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None
        
    def _init_schema(self) -> None:
        assert self.conn is not None, "DB not connected"
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def add_script(self, script: Script) -> None:
        """Insert a script. Raises sqlite3.IntegrityError if id/name conflict."""
        assert self.conn is not None, "DB not connected"

        created_at = script.created_at.isoformat() if script.created_at else _now_iso()
        updated_at = script.updated_at.isoformat() if script.updated_at else created_at

        with self._lock:
            self.conn.execute(
                """
                INSERT INTO scripts (id, name, description, path, hash, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    script.id,
                    script.name,
                    script.description,
                    script.path,
                    script.hash,
                    1 if script.enabled else 0,
                    created_at,
                    updated_at,
                ),
            )
            self.conn.commit()
    
    def get_script(self, script_id: str) -> Optional[Script]:
        assert self.conn is not None, "DB not connected"

        row = self._execute(
            "SELECT * FROM scripts WHERE id = ?",
            (script_id,),
        ).fetchone()

        return self._row_to_script(row) if row else None
    
    def get_script_by_name(self, name: str) -> Optional[Script]:
        assert self.conn is not None, "DB not connected"

        row = self._execute(
            "SELECT * FROM scripts WHERE name = ?",
            (name,),
        ).fetchone()

        return self._row_to_script(row) if row else None

    def _row_to_script(self, row: sqlite3.Row) -> Script:
        def parse_dt(s: Optional[str]) -> Optional[datetime]:
            return datetime.fromisoformat(s) if s else None
        
        return Script(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            path=row["path"],
            hash=row["hash"],
            enabled=bool(row["enabled"]),
            created_at=parse_dt(row["created_at"]),
            updated_at=parse_dt(row["updated_at"]),
        )
    
    def create_execution(self, script_id: str) -> str:
        """
        Create an execution row and return execution_id.
        """
        assert self.conn is not None, "DB not connected"

        execution_id = str(uuid.uuid4())
        started_at = _now_iso()

        with self._lock:
            self.conn.execute(
                """
                INSERT INTO executions (id, script_id, started_at, status)
                VALUES (?, ?, ?, ?)
                """,
                (execution_id, script_id, started_at, "running"),
            )
            self.conn.commit()
        return execution_id
    
    def complete_execution(
        self, 
        execution_id: str,
        *,
        exit_code: int,
        stdout: str,
        stderr: str,
        status: str = "completed",
    ) -> None:
        assert self.conn is not None, "DB not connected"

        completed_at = _now_iso()
        with self._lock:
            self.conn.execute(
                """
                UPDATE executions
                SET completed_at = ?, status = ?, exit_code = ?, stdout = ?, stderr = ?
                WHERE id = ?
                """,
                (completed_at, status, exit_code, stdout, stderr, execution_id),
            )
            self.conn.commit()
    
    def get_execution(self, execution_id: str) -> Optional[sqlite3.Row]:
        assert self.conn is not None, "DB not connected"
        return self._execute(
            "SELECT * FROM executions WHERE id = ?",
            (execution_id,),
        ).fetchone()
    
    def add_schedule(self, schedule_id: str, script_id: str, trigger_type: str, trigger_config: dict) -> None:
        assert self.conn is not None, "DB not connected"

        with self._lock:
            self.conn.execute(
                """
                INSERT INTO schedules (id, script_id, trigger_type, trigger_config)
                VALUES (? ,?, ?, ?)
                """,
                (schedule_id, script_id, trigger_type, _json.dumps(trigger_config),),
            )
            self.conn.commit()
    
    def get_schedules(self, script_id: str) -> list[dict]:
        assert self.conn is not None, "DB not connected"
        rows = self._execute(
            "SELECT * FROM schedules WHERE script_id = ?",
            (script_id,),
        ).fetchall()
    
        schedules: list[dict] = []
        for r in rows:
            schedules.append(
                {
                    "id": r["id"],
                    "script_id": r["script_id"],
                    "trigger_type": r["trigger_type"],
                    "trigger_config": _json.loads(r["trigger_config"]),
                }
            )
        return schedules
    
    def get_enabled_scripts(self) -> list:
        assert self.conn is not None, "DB not connected"
        rows = self._execute("SELECT * FROM scripts WHERE enabled = 1").fetchall()
        return [self._row_to_script(r) for r in rows]

    def get_last_execution_time(self, script_id: str):
        assert self.conn is not None, "DB not connected"
        row = self._execute(
            """
            SELECT started_at FROM executions
            WHERE script_id = ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (script_id,),
        ).fetchone()
        if not row:
            return None
        from datetime import datetime
        return datetime.fromisoformat(row["started_at"])

    def _execute(self, sql: str, params: tuple = ()):
        assert self.conn is not None, "DB not connected"
        with self._lock:
            return self.conn.execute(sql, params)
