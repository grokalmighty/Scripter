from pathlib import Path
from src.database import Database
from src.scripts_repo import add_script
from src.schedules_repo import add_cron_schedule, due_schedules

def test_cron_schedule_is_due_initially(tmp_path: Path):
    db = Database(tmp_path / "test.db")
    script_id = add_script(db, name="t", command="echo t")

    add_cron_schedule(db, script_id=script_id, cron="* * * * *", tz="America/New_York")

    due = due_schedules(db)
    assert len(due) == 1