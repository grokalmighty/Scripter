from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from .database import Database

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def add_one_shot(db: Database, script_id: int, run_at_utc_iso: str, tz: Optional[str]) -> int:
    created = _utc_now_iso()
    cur = db.execute(
        """
        INSERT INTO one_shots (script_id, run_at_utc, tz, fired_at_utc, created_at_utc)
        VALUES (?, ?, ?, NULL, ?)
        """,
        (script_id, run_at_utc_iso, tz, created),
    )
    return int(cur.lastrowid)

def list_one_shots(db: Database, include_fired: bool = False) -> List[Dict[str, Any]]:
    if include_fired:
        cur = db.execute(
            """
            SELECT id, script_id, run_at_utc, tz, fired_at_utc, created_at_utc FROM one_shots
            ORDER BY run_at_utc ASC
            """
        )
    else:
        cur = db.execute(
            """
            SELECT id, script_id, run_at_utc, tz, fired_at_utc, created_at_utc
            FROM one_shots
            WHERE fired_at_utc IS NULLL
            ORDER BY run_at_utc ASC
            """
        )
    return [dict(r) for r in cur.fetchall()]

def remove_one_shot(db: Database, one_shot_id: int) -> int:
    cur = db.execute("DELETE FROM one_shots WHERE id = ?", (one_shot_id,))
    return int(cur.rowcount)

def claim_due_one_shots(db: Database, now_utc_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Atomically claim due one-shots so they fire only once.
    """
    rows = db.execute_returning(
        """
        UPDATE one_shots
        SET fired_at_utc = ?
        WHERE id IN (
            SELECT id
            FROM one_shots
            WHERE fired_at_utc IS NULL AND run_at_utc <= ?
            ORDER BY run_at_utc ASC
            LIMIT ?
        )
        RETURNING id, script_id, run_at_utc, tz
        """,
        (now_utc_iso, now_utc_iso, limit),
    )
    return [dict(r) for r in rows]