from pathlib import Path
from src.database import Database
from src.scripts_repo import add_script
from src.runs_repo import create_run, finish_run

def test_create_and_finish_run(tmp_path: Path):
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    script_id = add_script(db, name="test", command="echo test")

    run_id = create_run(db, script_id=script_id)
    assert run_id > 0

    finish_run(
        db,
        run_id=run_id,
        status="success",
        exit_code=0,
        stdout="hello",
        stderr=""
    )

    rows = db.query("SELECT * FROM runs WHERE id = ?", (run_id,))
    assert len(rows) == 1

    row = rows[0]

    assert row["status"] == "success"
    assert row["exit_code"] == 0
    assert row["stdout"] == "hello"
    assert row["stderr"] == ""
    assert row["finished_at"] is not None