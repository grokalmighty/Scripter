from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from .database import Database

@dataclass(frozen=True)
class AppTrigger:
    id: int
    script_id: int
    process_name: str
    on_event: str
    created_at_utc: str

def add_app_trigger(db: Database, script_id: int, process_name: str, on_event: str) -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    cur = db.execute(
        "INSERT OR IGNORE INTO app_triggers(script_id, process_name, on_event, created_at_utc) "
        "VALUES(?,?,?,?)",
        (script_id, process_name, on_event, now),
    )
    return int(cur.lastrowid or 0)

def list_app_triggers(db: Database) -> list[AppTrigger]:
    rows = db.query(
        "SELECT id, script_id, process_name, on_event, created_at_utc "
        "FROM app_triggers ORDER BY process_name, on_event, id"
    )
    return [
        AppTrigger(
            id=int(r["id"]),
            script_id=int(r["script_id"]),
            process_name=str(r["process_name"]),
            on_event=str(r["on_event"]),
            created_at_utc=str(r["created_at_utc"]),
        )
        for r in rows
    ]

def remove_app_trigger(db: Database, trigger_id: int) -> int:
    cur = db.execute("DELETE FROM app_triggers WHERE id = ?", (trigger_id,))
    return int(cur.rowcount)