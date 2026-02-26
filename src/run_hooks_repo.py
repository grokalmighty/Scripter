from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from .database import Database

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def add_hook(db: Database, on_script_id: int, on_status: str, target_script_id: int) -> int:
    now = _utc_now_iso()
    cur = db.execute(
        """
        INSERT INTO run_hooks (on_script_id, on_status, target_script_id, created_at_utc)
        VALUES (?, ?, ?, ?)
        """,
        (on_script_id, on_status, target_script_id, now),
    )
    if cur.lastrowid:
        return int(cur.lastrowid)
    row = db.execute(
        "SELECT id FROM run_hooks WHERE on_script_id=? AND on_status=? AND target_script_id=?",
        (on_script_id, on_status, target_script_id),
    ).fetchone()
    return int(row["id"])

def list_hooks(db: Database) -> List[Dict[str, Any]]:
    cur = db.execute(
        """
        SELECT id, on_script_id, on_status, target_script_id, created_at_utc
        FROM run_hooks
        ORDER BY id ASC
        """
    )
    return [dict(r) for r in cur.fetchall()]

def remove_hook(db: Database, hook_id: int) -> int:
    cur = db.execute("DELETE FROM run_hooks WHERE id = ?", (hook_id,))
    return int(cur.rowcount)

def hooks_for(db: Database, on_script_id: int, status: str) -> List[Dict[str, Any]]:
    """
    status is the finished run status: 'success' or 'failed'
    """
    cur = db.execute(
        """
        SELECT id, target_script_id, on_status
        FROM run_hooks
        WHERE on_script_id = ?
            AND (on_status = ? OR on_status = 'any')
        """,
        (on_script_id, status),
    )
    return [dict(r) for r in cur.fetchall()]