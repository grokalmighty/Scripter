from __future__ import annotations
from datetime import datetime, timezone
from .database import Database

def add_daemon_hook(db: Database, event: str, script_id: int) -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    cur = db.execute(
        "INSERT OR IGNORE INTO daemon_hooks(event, script_id, created_at_utc) VALUES(?, ?, ?)",
        (event, script_id, now),
    )
    return int(cur.lastrowid or 0)

def list_daemon_hooks(db: Database) -> list[dict]:
    rows = db.query(
        "SELECT dh.id, dh.event, dh.script_id, s.name AS script_name, dh.created_at_utc "
        "FROM daemon_hooks dh JOIN scripts s ON s.id = dh.script_id "
        "ORDER BY dh.event, dh.id"
    )
    return [dict(r) for r in rows]

def hooks_for_event(db: Database, event: str) -> list[int]:
    rows = db.query("SELECT script_id FROM daemon_hooks WHERE event = ? ORDER BY id", (event,))
    return [int(r["script_id"]) for r in rows]

def remove_daemon_hook(db: Database, hook_id: int) -> int:
    cur = db.execute("DELETE FROM daemon_hooks WHERE id = ?", (hook_id,))
    return int(cur.rowcount)