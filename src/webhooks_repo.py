from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from .database import Database

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def add_webhook(db: Database, name: str, script_id: int) -> int:
    db.init()
    cur = db.execute(
        """
        INSERT INTO webhooks (name, script_id, created_at)
        VALUES (?, ?, ?)
        """,
        (name, script_id, _now_iso()),
    )
    return int(cur.lastrowid)

def list_webhooks(db: Database) -> int:
    db.init()
    return db.query(
        """
        SELECT w.id, w.name, w.script_id, s.name AS script_name, w.created_at
        FROM webhooks w
        JOIN scripts s ON s.id = w.script_id
        ORDER BY w.id ASC
        """
    )

def get_webhook(db: Database, name: str) -> Optional[dict]:
    db.init()
    rows = db.query(
        """
        SELECT w.id, w.name, w.script_id, s.name AS script_name
        FROM webhooks w
        JOIN scripts s ON s.id = w.script_id
        WHERE w.name = ?
        """,
        (name,),
    )
    return dict(rows[0]) if rows else None 

def remove_webhook(db: Database, name: str) -> int:
    db.init()
    cur = db.execute("DELETE FROM webhooks WHERE name = ?", (name,))
    return cur.rowcount