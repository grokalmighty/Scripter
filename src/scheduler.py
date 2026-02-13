from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

from .database import Database
from .schedules_repo import due_schedules, mark_run
from .scripts_repo import get_script
from .runs_repo import create_run, finish_run
from .executor import run_command

def run_loop(db_path: Optional[Path] = None, tick_seconds: int = 2) -> None:
    db = Database(db_path)
    db.init()

    while True:
        due = due_schedules(db)

        for sched in due:
            schedule_id = sched["id"]
            script_id = sched["script_id"]

            script = get_script(db, script_id)
            if script is None:
                mark_run(db, schedule_id)
                continue

            run_id = create_run(db, script_id)

            try:
                result = run_command(script.command, working_dir=script.working_dir)
                status = "success" if result.exit_code == 0 else "failed"
                finish_run(db, run_id, status, result.exit_code, result.stdout, result.stderr)
            except Exception as e:
                finish_run(db, run_id, "failed", None, "", f"{type(e).__name__}: {e}")

            mark_run(db, schedule_id)
        
        time.sleep(tick_seconds)