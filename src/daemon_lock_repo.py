from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from .database import Database

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def acquire_daemon_lock(db: Database, owner: str) -> None:
    pid = os.getpid()
    try:
        db.execute(
            "INSERT INTO daemon_lock(id, owner, pid, acquired_at_utc) VALUES (1, ?, ?, ?)",
            (owner, pid, _utc_now_iso()),
        )
    except Exception:
        row = db.query("SELECT owner, pid, acquired_at_utc FROM daemon_lock WHERE id = 1")
        holder = dict(row[0]) if row else {"owner": "unknown"}
        raise RuntimeError(f"Scheduler already running (lock held): {holder}")

def release_daemon_lock(db: Database, owner: str) -> None:
    db.execute("DELETE FROM daemon_lock WHERE id = 1 AND owner = ?", (owner,))


def get_daemon_lock(db: Database) -> Optional[Dict[str, Any]]:
    rows = db.query("SELECT owner, pid, acquired_at_utc FROM daemon_lock WHERE id = 1")
    return dict(rows[0]) if rows else None

def force_clear_daemon_lock(db: Database) -> int:
    cur = db.execute("DELETE FROM dameon_lock WHERE id = 1")
    return int(cur.rowcount)

