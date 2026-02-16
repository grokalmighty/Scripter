from __future__ import annotations

from pathlib import Path
from typing import Any
import yaml

from .database import Database

def export_config(db: Database, path: Path) -> None:
    db.init()

    scripts = db.query("SELECT id, name, command, working_dir FROM scripts ORDER BY id ASC")
    schedules = db.query("SELECT * FROM schedules ORDER BY id ASC")
    file_triggers = db.query("SELECT * FROM file_triggers ORDER BY id ASC")
    webhooks = db.query("SELECT * FROM webhooks ORDER BY id ASC")

    id_to_name = {s["id"]: s["name"] for s in scripts}

    out: dict[str, Any] = {
        "scripts": [
            {"name": s["name"], "command": s["command"], "cwd": s["working_dir"] or None}
            for s in scripts
        ],
        "schedules": [],
        "file_triggers": [],
        "webhooks": [],
    }

    for sch in schedules:
        script_ref = id_to_name.get(sch["script_id"], str(sch["script_id"]))

        cron = sch["cron"]  
        tz = sch["tz"]     
        interval = sch["interval_seconds"] 

        if cron:
            out["schedules"].append(
                {
                    "script": script_ref,
                    "cron": cron,
                    "tz": tz or None,
                }
            )
        elif interval is not None:
            out["schedules"].append(
                {
                    "script": script_ref,
                    "interval_seconds": int(interval),
                }
            )
    
    for ft in file_triggers:
        out["file_triggers"].append(
            {
            "script": id_to_name.get(ft["script_id"], str(ft["script_id"])),
            "path": ft["path"],
            "recursive": bool(ft["recursive"]),
            }
        )
    
    for w in webhooks:
        out["webhooks"].append(
            {
                "name": w["name"],
                "script": id_to_name.get(w["script_id"], str(w["script_id"])),
            }
        )
    
    path.write_text(yaml.safe_dump(out, sort_keys=False))