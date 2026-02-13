from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from .database import Database

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def create_run(db: Database, script_id: int, trigger: str | None = None) -> int:
    db.init()
    db.migrate()
    cur = db.execute(
        """
        INSERT INTO runs (script_id, status, started_at, trigger)
        VALUES (?, ?, ?, ?)
        """,
        (script_id, "running", _now_iso(), trigger),
    )
    return int(cur.lastrowid)

def finish_run(
    db: Database,
    run_id: int,
    status: str,
    exit_code: Optional[int],
    stdout: str,
    stderr: str,
) -> None:
    db.execute(
        """
        UPDATE runs
        SET status = ?, finished_at = ?, exit_code = ?, stdout = ?, stderr = ?
        WHERE id = ?
        """,
        (status, _now_iso(), exit_code, stdout, stderr, run_id),
    )

def list_runs(db: Database, limit: int = 20, script_id: Optional[int] = None):
    db.init()
    db.migrate()
    if script_id is None:
        return db.query(
            "SELECT * FROM runs ORDER BY id DESC LIMIT ?",
            (limit,),
        )
    return db.query(
        "SELECT * FROM runs WHERE script_id = ? ORDER BY id DESC LIMIT ?",
        (script_id, limit),
    )

def get_run(db: Database, run_id: int):
    db.init()
    db.migrate()
    rows = db.query("SELECT * FROM runs WHERE id = ?", (run_id,))
    return rows[0] if rows else None