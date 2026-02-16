from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional
from croniter import croniter
from zoneinfo import ZoneInfo
from .database import Database

def _now() -> datetime:
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
    now_utc = _now()

    for r in rows:
        interval = r["interval_seconds"]
        cron = r["cron"]
        tz = r["tz"]
        last_run = r["last_run"]

        if interval is not None:
            if last_run is None:
                due.append(r)
                continue
            last_dt = datetime.fromisoformat(last_run)
            if now_utc >= last_dt + timedelta(seconds=int(interval)):
                due.append(r)
            continue
        
        if cron:
            if tz:
                zone = ZoneInfo(tz)
            else:
                zone = datetime.now().astimezone().tzinfo
            
            now_local = now_utc.astimezone(zone)

            if last_run is None:
                base = now_local - timedelta(minutes=1)
            else:
                base = datetime.fromisoformat(last_run).astimezone(zone)
            
            it = croniter(cron, base)
            next_time = it.get_next(datetime)

            if next_time <= now_local:
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
    return db.query(
        """
        SELECT
            sc.id,
            sc.script_id,
            s.name AS script_name,
            sc.interval_seconds,
            sc.cron,
            sc.tz,
            sc.last_run
        FROM schedules sc
        JOIN scripts s ON s.id = sc.script_id
        ORDER BY sc.id ASC
        """
    )

def add_cron_schedule(db: Database, script_id: int, cron: str, tz: Optional[str] = None) -> int:
    db.init()
    now = _now().isoformat()
    cur = db.execute(
        """
        INSERT INTO schedules (script_id, interval_seconds, cron, tz, last_run, created_at)
        VALUES (?, NULL, ?, ?, NULL, ?)
        """,
        (script_id, cron, tz, now),
    )
    return int(cur.lastrowid)