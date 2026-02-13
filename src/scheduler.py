from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from .database import Database
from .schedules_repo import due_schedules, mark_run
from .scripts_repo import get_script
from .runs_repo import create_run, finish_run
from .executor import run_command
from .file_triggers_repo import list_file_triggers
from .file_watcher import FileWatcher 

def run_loop(db_path: Optional[Path] = None, tick_seconds: int = 2, once: bool = False) -> None:
    db = Database(db_path)
    db.init()
    
    watcher = FileWatcher()

    while True:
        due = due_schedules(db)

        for sched in due:
            schedule_id = sched["id"]
            script_id = sched["script_id"]

            script = get_script(db, script_id)
            if script is None:
                mark_run(db, schedule_id)
                continue
            
            run_id = create_run(db, script_id, trigger=f"schedule:{schedule_id}")

            try:
                result = run_command(script.command, working_dir=script.working_dir)
                status = "success" if result.exit_code == 0 else "failed"
                finish_run(db, run_id, status, result.exit_code, result.stdout, result.stderr)
            except Exception as e:
                finish_run(db, run_id, "failed", None, "", f"{type(e).__name__}: {e}")

            mark_run(db, schedule_id)
        
        file_triggers = list_file_triggers(db)

        if not hasattr(run_loop, "_last_change_seen"):
            run_loop._last_change_seen = {}
            run_loop._last_executed_for_burst = {}
            run_loop._last_file_exec_time = {}
        
        last_change_seen: dict[int, datetime] = run_loop._last_change_seen
        last_executed_for_change: dict[int, datetime] = run_loop._last_executed_for_burst
        last_file_exec_time: dict[int, datetime] = run_loop._last_file_exec_time
        QUIET_SECONDS = 3
        MIN_INTERVAL_SECONDS = 30

        for ft in file_triggers:
            ft_id = ft["id"]
            script_id = ft["script_id"]
            path = ft["path"]
            recursive = bool(ft["recursive"])

            now = datetime.now(timezone.utc)

            changed = watcher.scan(path, recursive)
            if changed:
                last_change_seen[ft_id] = now
                continue

            last_change = last_change_seen.get(ft_id)
            if last_change is None:
                continue

            if (now - last_change).total_seconds() < QUIET_SECONDS:
                continue

            last_exec = last_executed_for_change.get(ft_id)
            if last_exec is not None and last_exec >= last_change:
                continue

            script = get_script(db, script_id)
            if script is None:
                continue
            
            last_time = last_file_exec_time.get(ft_id)
            if last_time is not None and (now - last_time).total_seconds() < MIN_INTERVAL_SECONDS:
                continue

            last_file_exec_time[ft_id] = now

            run_id = create_run(db, script_id, trigger=f"file:{ft_id}")
            try:
                result = run_command(script.command, working_dir=script.working_dir)
                status = "success" if result.exit_code == 0 else "failed"
                finish_run(db, run_id, status, result.exit_code, result.stdout, result.stderr)
            except Exception as e:
                finish_run(db, run_id, "failed", None, "", f"{type(e).__name__}: {e}")

            last_executed_for_change[ft_id] = now
        if once:
            return
        time.sleep(tick_seconds)