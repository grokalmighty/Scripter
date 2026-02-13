from __future__ import annotations

from datetime import datetime, timezone
from .database import Database

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def add_file_trigger(db: Database, script_id: int, path: str, recursive: bool = False) -> int:
    db.init()
    cur = db.execute(
        """
        INSERT INTO file_triggers (script_id, path, recursive, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (script_id, path, int(recursive), _now_iso()),
    )
    return int(cur.lastrowid)

def list_file_triggers(db: Database):
    db.init()
    return db.query(
        """
        SELECT ft.id, ft.script_id, s.name as script_name, ft.path, ft.recursive
        FROM file_triggers ft
        JOIN scripts s ON s.id = ft.script_id
        ORDER BY ft.id ASC"""
    )

def remove_file_trigger(db: Database, trigger_id: int) -> int:
    db.init()
    cur = db.execute("DELETE FROM file_triggers WHERE id = ?", (trigger_id,))
    return cur.rowcount