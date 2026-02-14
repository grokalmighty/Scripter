from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional
import sqlite3

from .database import Database

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def owner_id() -> str:
    return f"{os.uname().nodename}:{os.getpid()}"

def try_acquire(db: Database, key: str, owner: str) -> bool:
    db.init()
    try:
        db.execute(
            """
            INSERT INTO locks (key, owner, acquired_at)
                VALUES (?, ?, ?)
            """,
            (key, owner, _now_iso()),
        )
        return True
    except sqlite3.IntegrityError:
        return False

def release(db: Database, key: str, owner: str) -> None:
    db.execute(
        "DELETE FROM locks WHERE key = ? AND owner = ?",
        (key, owner),
    )