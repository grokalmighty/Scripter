from pathlib import Path
from src.database import Database
from src.scripts_repo import add_script
from src.daemon_hooks_repo import add_daemon_hook
from src.scheduler import run_loop
from src.runs_repo import list_runs

def test_daemon_start_hook_runs(tmp_path: Path):
    db_path = tmp_path / "test.db"
    db = Database(db_path)
    db.init()

    s = add_script(db, name="on_start", command="echo started")
    add_daemon_hook(db, event="start", script_id=s)

    run_loop(db_path=db_path, tick_seconds=0, once=True)

    runs = list_runs(db, limit=20)
    start_runs = [r for r in runs if r["script_id"] == s and r["trigger"] == "daemon:start"]
    assert len(start_runs) == 1
    assert start_runs[0]["status"] == "success"