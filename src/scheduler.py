from __future__ import annotations

import time
from pathlib import Path
from typing import Optional, Iterable

from .database import Database
from .executor import run_command
from .locks import try_acquire, release, owner_id
from .runs_repo import create_run, finish_run
from .scripts_repo import get_script
from .trigger_sources.base import TriggerSource
from .trigger_sources.file_watch import FileWatchSource
from .trigger_sources.schedules import ScheduleSource
from .triggers.base import TriggerEvent

def _execute_event(db: Database, event: TriggerEvent, owner: str) -> None:
    """
    Single place where a TriggerEvent becomes a run.
    """
    script = get_script(db, event.script_id)
    if script is None:
        return 
    
    lock_key = f"script:{event.script_id}"
    if not try_acquire(db, lock_key, owner):
        return 
    
    run_id = create_run(db, event.script_id, trigger=event.trigger_id)

    try:
        result = run_command(script.command, working_dir=script.working_dir)
        status = "success" if result.exit_code == 0 else "failed"
        finish_run(db, run_id, status, result.exit_code, result.stdout, result.stderr)
    except Exception as e:
        finish_run(db, run_id, "failed", None, "", f"{type(e).__name__}: {e}")
    finally:
        release(db, lock_key, owner)

def run_loop(db_path: Optional[Path] = None, 
             tick_seconds: int = 2, 
             once: bool = False,
             sources: Optional[Iterable[TriggerSource]] = None,) -> None:
    db = Database(db_path)
    db.init()

    owner = owner_id()
    
    active_sources: list[TriggerSource] = list(
        sources if sources is not None else [ScheduleSource(), FileWatchSource()]
    )

    while True:
        for source in active_sources:
            for event in source.poll(db):
                _execute_event(db, event, owner)
    
        if once:
            return
        time.sleep(tick_seconds)