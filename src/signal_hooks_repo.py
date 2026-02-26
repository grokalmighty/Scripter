from __future__ import annotations
from datetime import datetime, timezone
from .database import Database

def add_signal_hook(db: Database, sig: str, script_id: int) -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    cur = db.execute(
        "INSERT OR IGNORE INTO signal_hooks(signal, script_id, created_at_utc) VALUES(?, ?, ?)",
        (sig, script_id, now),
    )
    return int(cur.lastrowid or 0)

def list_signal_hooks(db: Database) -> list[dict]:
    rows = db.query(
        "SELECT sh.id, sh.signal, sh.script_id, s.name AS script_name, sh.created_at_utc "
        "FROM signal_hooks sh JOIN scripts s ON s.id = sh.script_id "
        "ORDER BY sh.signal, sh.id"
    )
    return [dict(r) for r in rows]

def hooks_for_signal(db: Database, sig: str) -> list[int]:
    rows = db.query("SELECT script_id FROM signal_hooks WHERE signal = ? ORDER BY id", (sig,))
    return [int(r["script_id"]) for r in rows]

def remove_signal_hook(db: Database, hook_id: int) -> int:
    cur = db.execute("DELETE FROM signal_hooks WHERE id = ?", (hook_id,))
    return int(cur.rowcount)