from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .database import Database

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def enqueue_event(db: Database, trigger_id: str, script_id: int, payload: Optional[Dict[str, Any]] = None) -> int:
    now = _utc_now_iso()
    payload_json = json.dumps(payload) if payload is not None else None
    cur = db.execute(
        """
        INSERT INTO pending_events (trigger_id, script_id, payload_json, created_at_utc, claimed_at_utc, claimed_by, processed_at_utc)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL)
        """,
        (trigger_id, script_id, payload_json, now),
    )
    return int(cur.lastrowid)

def claim_ready_events(db: Database, owner: str, limit: int = 50) -> List[Dict[str, Any]]:
    now = _utc_now_iso()
    rows = db.execute_returning(
        """
        UPDATE pending_events
        SET claimed_at_utc = ?, claimed_by = ?
        WHERE id IN (
            SELECT id
            FROM pending_events
            WHERE processed_at_utc IS NULL AND claimed_at_utc IS NULL
            ORDER BY id ASC
            LIMIT ?
        )
        RETURNING id, trigger_id, script_id, payload_json
        """,
        (now, owner, limit),
    )
    return [dict(r) for r in rows]

def mark_processed(db: Database, pending_id: int) -> None:
    now = _utc_now_iso()
    db.execute(
        "UPDATE pending_events SET processed_at_utc = ? WHERE id = ?",
        (now, pending_id),
    )