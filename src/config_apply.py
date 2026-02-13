from __future__ import annotations

from pathlib import Path
from typing import Any
import yaml

from .database import Database
from .scripts_repo import add_script, list_scripts
from .schedules_repo import add_schedule

def apply_config(db: Database, path: Path) -> None:
    db.init()
    data: dict[str, Any] = yaml.safe_load(path.read_text()) or {}

    scripts = data.get("scripts", [])
    schedules = data.get("schedules", [])

    existing = {s.name: s.id for s in list_scripts(db)}

    for s in scripts:
        name = s["name"]
        if name in existing:
            continue
        sid = add_script(
            db,
            name=name,
            command=s["command"],
            working_dir=s.get("cwd"),
        )
        existing[name] = sid
    
    for sch in schedules:
        script_name = sch["script"]
        if script_name not in existing:
            raise ValueError(f"Schedule references unknown script: {script_name}")
        add_schedule(
            db,
            script_id=existing[script_name],
            interval_seconds=int(sch["interval_seconds"]),
        )