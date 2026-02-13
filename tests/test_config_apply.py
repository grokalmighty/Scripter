from pathlib import Path
from src.database import Database
from src.config_apply import apply_config
from src.scripts_repo import list_scripts
from src.schedules_repo import list_schedules

def test_apply_config_creates_script_and_schedule(tmp_path: Path):
    db = Database(tmp_path / "test.db")

    cfg = tmp_path / "scripter.yml"
    cfg.write_text(
        """
scripts:
  - name: hello
    command: echo hello
    cwd: null

schedules:
  - script: hello
    interval_seconds: 10
"""
    )

    apply_config(db, cfg)

    scripts = list_scripts(db)
    assert len(scripts) == 1
    assert scripts[0].name == "hello"

    schedules = list_schedules(db)
    assert len(schedules) == 1
    assert schedules[0]["script_name"] == "hello"
    assert schedules[0]["interval_seconds"] == 10