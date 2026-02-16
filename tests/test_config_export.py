from pathlib import Path
import yaml

from src.database import Database
from src.scripts_repo import add_script
from src.schedules_repo import add_schedule, add_cron_schedule
from src.file_triggers_repo import add_file_trigger
from src.webhooks_repo import add_webhook
from src.config_export import export_config


def test_export_config_writes_yaml(tmp_path: Path):
    db = Database(tmp_path / "t.db")
    sid = add_script(db, name="hello", command="echo hello")

    add_schedule(db, script_id=sid, interval_seconds=10)
    add_cron_schedule(db, script_id=sid, cron="* * * * *", tz="America/New_York")
    add_file_trigger(db, script_id=sid, path="watched", recursive=False)
    add_webhook(db, name="hello", script_id=sid)

    out = tmp_path / "out.yml"
    export_config(db, out)

    data = yaml.safe_load(out.read_text())
    assert "scripts" in data and len(data["scripts"]) == 1
    assert "schedules" in data and len(data["schedules"]) >= 1
    assert "file_triggers" in data and len(data["file_triggers"]) == 1
    assert "webhooks" in data and len(data["webhooks"]) == 1
