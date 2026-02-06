from __future__ import annotations

import sqlite3
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