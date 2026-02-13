from src.database import Database
from src.scripts_repo import add_script
from src.schedules_repo import add_schedule, due_schedules

def test_add_due_schedule(tmp_path):
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    script_id = add_script(db, name="test", command="echo test")

    sid = add_schedule(db, script_id=script_id, interval_seconds=60)
    assert sid > 0

    due = due_schedules(db)
    assert len(due) == 1