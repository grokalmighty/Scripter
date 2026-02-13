from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional
from .database import Database

def _now():
    return datetime.now(timezone.utc)

def add_schedule(db: Database, script_id: int, interval_seconds: int) -> int:
    db.init()
    now = _now().isoformat()
    cur = db.execute(
        """
        INSERT INTO schedules (script_id, interval_seconds, last_run, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (script_id, interval_seconds, None, now),
    )
    return int(cur.lastrowid)

def due_schedules(db: Database):
    db.init()
    rows = db.query("SELECT * FROM schedules")
    due = []
    now = _now()

    for r in rows:
        last_run = r["last_run"]
        interval = r["interval_seconds"]

        if last_run is None:
            due.append(r)
            continue

        last_dt = datetime.fromisoformat(last_run)
        if now >= last_dt + timedelta(seconds=interval):
            due.append(r)
        
    return due 

def mark_run(db: Database, schedule_id: int):
    now = _now().isoformat()
    db.execute(
        "UPDATE schedules SET last_run = ? WHERE id = ?",
        (now, schedule_id),
    )