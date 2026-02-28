from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from .scripts_repo import get_script
import sqlite3

from .database import Database

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def enqueue_event(db: Database, trigger_id: str, script_id: int, payload: Optional[Dict[str, Any]] = None) -> Optional[int]:
    now = _utc_now_iso()
    payload_json = json.dumps(payload) if payload is not None else None

    s = get_script(db, script_id)
    policy = getattr(s, "concurrency_policy", "allow") or "allow"

    if policy == "queue_one":
        try:
            rows = db.execute_returning(
                """
                INSERT INTO pending_events
                  (trigger_id, script_id, payload_json, created_at_utc,
                   claimed_at_utc, claimed_by, processed_at_utc, queue_tag)
                VALUES (?, ?, ?, ?, NULL, NULL, NULL, 'queue_one')
                RETURNING id
                """,
                (trigger_id, script_id, payload_json, now),
            )
            return int(rows[0]["id"]) if rows else None
        except sqlite3.IntegrityError:
            return None

    cur = db.execute(
        """
        INSERT INTO pending_events
          (trigger_id, script_id, payload_json, created_at_utc,
           claimed_at_utc, claimed_by, processed_at_utc, queue_tag)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL)
        """,
        (trigger_id, script_id, payload_json, now),
    )
    return int(cur.lastrowid)

def claim_ready_events(db: Database, owner: str, limit: int = 50) -> List[Dict[str, Any]]:
    now = _utc_now_iso()
    rows = db.execute_returning(
        """
        WITH picked AS (
            SELECT MIN(id) AS id
            FROM pending_events
            WHERE processed_at_utc IS NULL
              AND claimed_at_utc IS NULL
            GROUP BY script_id
            ORDER BY MIN(id) ASC
            LIMIT ?
        )
        UPDATE pending_events
        SET claimed_at_utc = ?, claimed_by = ?
        WHERE id IN (SELECT id FROM picked)
        RETURNING id, trigger_id, script_id, payload_json
        """,
        (limit, now, owner),
    )
    return [dict(r) for r in rows]

def mark_processed(db: Database, pending_id: int) -> None:
    now = _utc_now_iso()
    db.execute(
        "UPDATE pending_events SET processed_at_utc = ? WHERE id = ?",
        (now, pending_id),
    )

def has_pending_event(db: Database, script_id: int) -> bool:
    rows = db.query(
        """
        SELECT 1 
        FROM pending_events 
        WHERE script_id = ? 
            AND processed_at_utc IS NULL 
            AND claimed_at_utc IS NULL
        LIMIT 1
        """,
        (script_id,),
    )
    return bool(rows)

def has_other_pending_event(db: Database, script_id: int, exclude_id: int) -> bool:
    rows = db.query(
        """
        SELECT 1
        FROM pending_events
        WHERE script_id = ?
          AND processed_at_utc IS NULL
          AND claimed_at_utc IS NULL
          AND id != ?
        LIMIT 1
        """,
        (script_id, exclude_id),
    )
    return bool(rows)

def unclaim_event(db: Database, pending_id: int) -> None:
    db.execute(
        "UPDATE pending_events SET claimed_at_utc = NULL, claimed_by = NULL WHERE id = ?",
        (pending_id,),
    )

def drop_extra_waiting_events(db: Database, script_id: int, keep: int = 1) -> int:
    """
    Keep at most `keep` waiting (unclaimed) pending_events for a script.
    Marks older extras as processed. Returns number dropped.
    """
    now = _utc_now_iso()
    cur = db.execute(
        """
        UPDATE pending_events
        SET processed_at_utc = ?
        WHERE id IN (
            SELECT id FROM pending_events
            WHERE script_id = ?
                AND processed_at_utc IS NULL
                AND claimed_at_utc IS NULL
            ORDER BY id ASC
            LIMIT -1 OFFSET ?
        )
        """,
        (now, script_id, keep),
    )
    return cur.rowcount

def trim_queue_one(db: Database, script_id: int) -> None:
    """
    Ensure at most ONE unprocessed+unclaimed pending row
    exists for this script.
    """
    now = _utc_now_iso()
    db.execute(
        """
        UPDATE pending_events
        SET processed_at_utc = ?
        WHERE id IN (
            SELECT id FROM pending_events
            WHERE script_id = ?
              AND processed_at_utc IS NULL
              AND claimed_at_utc IS NULL
            ORDER BY id ASC
            LIMIT -1 OFFSET 1
        )
        """,
        (now, script_id),
    )

def enqueue_event_if_none_waiting(db: Database, trigger_id: str, script_id: int, payload=None) -> Optional[int]:
    now = _utc_now_iso()
    payload_json = json.dumps(payload) if payload is not None else None
    try:
        row = db.execute_returning(
            """
            INSERT INTO pending_events (trigger_id, script_id, payload_json, created_at_utc, claimed_at_utc, claimed_by, processed_at_utc)
            VALUES (?, ?, ?, ?, NULL, NULL, NULL)
            RETURNING id
            """,
            (trigger_id, script_id, payload_json, now),
        )
        return int(row[0]["id"]) if row else None
    except sqlite3.IntegrityError:
        return None

def has_claimed_unprocessed_event(db: Database, script_id: int) -> bool:
    rows = db.query(
        """
        SELECT 1
        FROM pending_events
        WHERE script_id = ?
          AND processed_at_utc IS NULL
          AND claimed_at_utc IS NOT NULL
        LIMIT 1
        """,
        (script_id,),
    )
    return bool(rows)

def enqueue_event_if_under_cap(
    db: Database,
    trigger_id: str,
    script_id: int,
    cap: int,
    payload: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    now = _utc_now_iso()
    payload_json = json.dumps(payload) if payload is not None else None

    row = db.execute_returning(
        """
        INSERT INTO pending_events (trigger_id, script_id, payload_json, created_at_utc, claimed_at_utc, claimed_by, processed_at_utc)
        SELECT ?, ?, ?, ?, NULL, NULL, NULL
        WHERE (
            SELECT COUNT(*)
            FROM pending_events
            WHERE script_id = ?
              AND processed_at_utc IS NULL
        ) < ?
        RETURNING id
        """,
        (trigger_id, script_id, payload_json, now, script_id, cap),
    )
    return int(row[0]["id"]) if row else None

import sqlite3, json
from typing import Optional, Dict, Any

def enqueue_queue_one(db, trigger_id: str, script_id: int, payload: Optional[Dict[str, Any]] = None) -> Optional[int]:
    now = _utc_now_iso()
    payload_json = json.dumps(payload) if payload is not None else None
    try:
        rows = db.execute_returning(
            """
            INSERT INTO pending_events
              (trigger_id, script_id, payload_json, created_at_utc, claimed_at_utc, claimed_by, processed_at_utc, queue_tag)
            VALUES (?, ?, ?, ?, NULL, NULL, NULL, 'queue_one')
            RETURNING id
            """,
            (trigger_id, script_id, payload_json, now),
        )
        return int(rows[0]["id"]) if rows else None
    except sqlite3.IntegrityError:
        # unique index says: already has one waiting queue_one row
        return None