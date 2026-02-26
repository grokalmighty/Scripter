from pathlib import Path

from src.database import Database
from src.scripts_repo import add_script
from src.run_hooks_repo import add_hook
from src.scheduler import run_loop
from src.runs_repo import list_runs


def test_run_hook_triggers_target_script(tmp_path: Path):
    db_path = tmp_path / "test.db"
    db = Database(db_path)
    db.init()

    script_a = add_script(db, name="a", command="echo A")
    script_b = add_script(db, name="b", command="echo B")

    add_hook(db, on_script_id=script_a, on_status="success", target_script_id=script_b)

    from src.triggers.base import TriggerEvent
    from src.run_service import execute_event
    from src.locks import owner_id

    execute_event(
        db,
        TriggerEvent(trigger_id="manual", script_id=script_a),
        owner_id(),
    )

    run_loop(db_path=db_path, tick_seconds=0, once=True)

    runs = list_runs(db, limit=10)

    hook_runs = [
        r for r in runs
        if r["script_id"] == script_b and r["trigger"].startswith("hook:")
    ]

    assert len(hook_runs) == 1
    assert hook_runs[0]["status"] == "success"
    assert hook_runs[0]["exit_code"] == 0