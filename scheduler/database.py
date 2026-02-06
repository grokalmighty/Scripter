from __future__ import annotations

import sqlite3
import uuid

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional
from pathlib import Path

from .models import Script

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self._init_schema()

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
    
    def _init_schema(self) -> None:
        assert self.conn is not None, "Call connect() first"

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scripts (
                id TEXT PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                description TEXT,
                path TEXT NOT NULL,
                hash TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT,
                updated_at TEXT);"""
        )
        self.conn.commit()

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                script_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                FOREIGN KEY (script_id) REFERENCES scripts(id));
            """
        )
        self.conn.commit()
    
    def add_script(self, script: Script) -> None:
        assert self.conn is not None, "Call connect() first"

        created = script.created_at.isoformat() if script.created_at else _utcnow_iso()
        updated = script.updated_at.isoformat() if script.updated_at else created

        self.conn.execute(
            """
            INSERT INTO scripts (id, name, description, path, hash, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
        (
            script.id,
            script.name,
            script.description,
            script.path,
            script.hash,
            1 if script.enabled else 0,
            created,
            updated,
        ),
        )
        self.conn.commit()

    def get_script(self, script_id: str) -> Optional[Script]:
        assert self.conn is not None, "Call connect() first"

        row = self.conn.execute(
            "SELECT * FROM scripts WHERE id = ?;",
            (script_id,),
        ).fetchone()

        return self._row_to_script(row) if row else None

    def get_script_name(self, name: str) -> Optional[Script]:
        assert self.conn is not None, "Call connect() first"

        row = self.conn.execute(
            "SELECT * FROM scripts WHERE name =?;",
            (name,),
        ).fetchone()

        return self._row_to_script(row) if row else None
    
    def _row_to_script(self, row: sqlite3.Row) -> Script:
        created_at = datetime.fromisoformat(row["created_at"]) if row["created_at"] else None
        updated_at = datetime.fromisoformat(row["updated_at"]) if row["updated_at"] else None

        return Script(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            path=row["path"],
            hash=row["hash"],
            enabled=bool(row["enabled"]),
            created_at=created_at,
            updated_at=updated_at,
        )
    
    @classmethod
    def get_default(cls) -> "Database":
        base_dir = Path.home() / ".scheduler"
        base_dir.mkdir(parents=True, exist_ok=True)
        db_path = base_dir / "scheduler.db"
        db = cls(str(db_path))
        db.connect()
        return db
    
    def create_execution(self, script_id: str) -> str:
        assert self.conn is not None, "Call connect() first"

        execution_id = str(uuid.uuid4())
        started_at = _utcnow_iso()

        self.conn.execute(
            """
            INSERT INTO executions (id, script_id, started_at, status)
            VALUES (?, ?, ?, ?);
            """,
            (execution_id, script_id, started_at, "running"),
        )
        self.conn.commit()
        return execution_id
    
    def complete_execution(
         self,
         execution_id: str,
         *,
         status: str,
         exit_code: int | None = None,
         stdout: str | None = None,
         stderr: str | None = None,   
    ) -> None:
        assert self.conn is not None, "Call connect() first"

        completed_at = _utcnow_iso()

        self.conn.execute(
            """
            UPDATE executions
            SET completed_at = ?,
                status = ?,
                exit_code = ?,
                stdout = ?,
                stderr = ?
            WHERE id = ?;""",
            (completed_at, status, exit_code, stdout, stderr, execution_id),
        )
        self.conn.commit()
    
    def get_last_execution_row(self, script_id: str):
        assert self.conn is not None, "Call connect() first"

        return self.conn.execute(
            """
            SELECT *
            FROM executions
            WHERE script_id = ?
            ORDER BY started_at DESC
            LIMIT 1;""",
            (script_id,),
        ).fetchone()
