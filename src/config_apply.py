from __future__ import annotations

from pathlib import Path
from typing import Any
import yaml

from .database import Database
from .scripts_repo import add_script, list_scripts
from .schedules_repo import add_schedule, add_cron_schedule
from .file_triggers_repo import add_file_trigger
from .webhooks_repo import add_webhook

def apply_config(db: Database, path: Path) -> None:
    db.init()
    data = yaml.safe_load(path.read_text()) or {}

    scripts = data.get("scripts", [])
    schedules = data.get("schedules", [])

    existing = {s.name: s.id for s in list_scripts(db)}

    name_to_id: dict[str, int] = {}
    for s in data.get("scripts", []):
        sid = add_script(
            db,
            name=s["name"],
            command=s["command"],
            working_dir=s.get("cwd"),
        )
        name_to_id[s["name"]] = sid

    def resolve_script(ref):
        # ref can be a name ("hello") or an int id
        if isinstance(ref, int):
            return ref
        if isinstance(ref, str) and ref.isdigit():
            return int(ref)
        return name_to_id[ref]
    
    for sch in data.get("schedules", []):
        script_id = resolve_script(sch["script"])

        if "cron" in sch:
            add_cron_schedule(
                db,
                script_id=script_id,
                cron=sch["cron"],
                tz=sch.get("tz"),
            )
        else:
            add_schedule(
                db,
                script_id=script_id,
                interval_seconds=int(sch["interval_seconds"]),
            )
    for ft in data.get("file_triggers", []):
        script_id = resolve_script(ft["script"])
        add_file_trigger(
            db,
            script_id=script_id,
            path=ft["path"],
            recursive=bool(ft.get("recursive", False)),
        )

    for w in data.get("webhooks", []):
        script_id = resolve_script(w["script"])
        add_webhook(
            db,
            name=w["name"],
            script_id=script_id,
        )
