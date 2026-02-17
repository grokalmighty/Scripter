from __future__ import annotations
from typing import Callable, Optional

from .database import Database
from .executor import run_command
from .locks import try_acquire, release
from .runs_repo import create_run, finish_run
from .scripts_repo import get_script
from .triggers.base import TriggerEvent

def execute_event(db: Database, event: TriggerEvent, owner: str, on_finished: Optional[Callable[[str, int | None], None]] = None):
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
        if on_finished:
            on_finished(status, run_id)
    except Exception as e:
        finish_run(db, run_id, "failed", None, "", f"{type(e).__name__}: {e}")
        if on_finished:
            on_finished("failed", run_id)
    finally:
        release(db, lock_key, owner)