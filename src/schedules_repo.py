from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional
from .database import Database

def _now():
    return datetime.now(timezone.utc)

def add_schedule(db: Database, script_id: int, interval_seconds: int) -> int:
    db.init()
    db.migrate()
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
    db.migrate()
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

def list_schedules(db: Database):
    db.init()
    db.migrate()
    return db.query(
        """
        SELECT s.id, s.script_id, sc.name as script_name, s.interval_seconds, s.last_run, s.created_at
        FROM schedules s
        JOIN scripts sc ON sc.id = s.script_id
        ORDER BY s.id ASC"""
    )