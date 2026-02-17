from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from .database import Database

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def publish_event(db: Database, topic: str, payload_json: Optional[str]) -> int:
    now = _utc_now_iso()
    cur = db.execute(
        """
        INSERT INTO events (topic, payload_json, created_at_utc)
        VALUES (?, ?, ?)
        """,
        (topic, payload_json, now),
    )
    event_id = int(cur.lastrowid)

    db.execute(
        """
        INSERT OR IGNORE INTO deliveries (event_id, subscription_id, claimed_at_utc, claimed_by, processed_at_utc)
        SELECT ?, s.id, NULL, NULL, NULL
        FROM subscriptions s
        WHERE s.topic = ?
        """,
        (event_id, topic),
    )
    return event_id

def list_events(db: Database):
    db.init()
    return db.query(
        """
        SELECT e.id, e.topic
        FROM events e
        JOIN scripts s ON s.id = e.id
        ORDER BY e.id ASC
        """
    )

def subscribe(db: Database, topic: str, script_id: int) -> int:
    now = _utc_now_iso()
    cur = db.execute(
        """
        INSERT OR IGNORE INTO subscriptions (topic, script_id, created_at_utc) VALUES (?, ?, ?)
        """,
        (topic, script_id, now),
    )

    if cur.lastrowid:
        sub_id = int(cur.lastrowid)
    else:
        row = db.execute(
            """
            SELECT id 
            FROM subscriptions 
            WHERE topic = ? AND script_id = ?
            """,
            (topic, script_id),
        ).fetchone()
        sub_id = int(row["id"])
    
    db.execute(
        """
        INSERT OR IGNORE INTO deliveries (event_id, subscription_id, claimed_at_utc, claimed_by, processed_at_utc)
        SELECT e.id, ?, NULL, NULL, NULL
        FROM events e
        WHERE e.topic = ?
        """,
        (sub_id, topic),
    )

    return sub_id

def list_subscriptions(db: Database):
    db.init()
    return db.query(
        """
        SELECT s.id, s.topic
        FROM subscriptions s
        JOIN scripts t ON s.script_id = t.id 
        ORDER BY e.id ASC
        """
    )

def claim_ready_deliveries(db: Database, owner: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Claim deliveries that are:
        - not processed
        - not claimed
    Return enough info to run scripts
    """

    now = _utc_now_iso()

    rows = db.execute_returning(
        """
        UPDATE deliveries
        SET claimed_at_utc = ?, claimed_by = ?
        WHERE id IN (
            SELECT d.id
            FROM deliveries d
            WHERE d.processed_at_utc IS NULL
                AND d.claimed_at_utc IS NULL
            ORDER BY d.id ASC
            LIMIT ?
        )
        RETURNING id, event_id, subscription_id
        """,
        (now, owner, limit),
    )

    if not rows:
        return []
    
    delivery_ids = [int(r["id"]) for r in rows]

    qmarks = ",".join("?" for _ in delivery_ids)
    cur = db.execute(
        f"""
        SELECT 
            d.id AS delivery_id,
            d.event_id,
            s.script_id,
            e.topic,
            e.payload_json
        FROM deliveries d
        JOIN subscriptions s ON s.id = d.subscription_id
        JOIN events e ON e.id = d.event_id
        WHERE d.id IN ({qmarks})
        """,
        tuple(delivery_ids),
    )
    return [dict(r) for r in cur.fetchall()]

def mark_delivery_processed(db: Database, delivery_id: int) -> None:
    now = _utc_now_iso()
    db.execute(
        """
        UPDATE deliveries SET processed_at_utc = ? WHERE id = ?
        """,
        (now, delivery_id),
    )