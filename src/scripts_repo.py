from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from .database import Database
from .models import Script

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def add_script(db: Database, name: str, command: str, working_dir: Optional[str] = None) -> int:
    db.init()
    now = _now_iso()
    cur = db.execute(
        """
        INSERT INTO scripts (name, command, working_dir, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (name, command, working_dir, now, now),
    )
    return int(cur.lastrowid)

def list_scripts(db: Database) -> list[Script]:
    db.init()
    rows = db.query("SELECT * FROM scripts ORDER BY id ASC")
    return [Script(**dict(r)) for r in rows]

def get_script(db: Database, script_id: int) -> Optional[Script]:
    db.init()
    rows = db.query("SELECT * FROM scripts WHERE id = ?", (script_id,))
    if not rows:
        return None
    return Script(**dict(rows[0]))