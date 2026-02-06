from pathlib import Path

from scheduler.database import Database
from scheduler.executor import ScriptExecutor
from scheduler.models import Script

def test_execute_script_records_execution(tmp_path: Path):
    script_path = tmp_path / "hello.sh"
    script_path.write_text("#!/bin/sh\necho 'Hello World'\n")
    script_path.chmod(0o755)

    db = Database(":memory:")
    db.connect()

    s = Script(id="s1", name="hello", path=str(script_path))
    db.add_script(s)

    executor = ScriptExecutor(db)
    res = executor.execute("s1")

    assert res.returncode == 0
    assert "Hello World" in res.stdout

    row = db.get_last_execution_row("s1")
    assert row is not None
    assert row["status"] == "success"
    assert row["exit_code"] == 0
    assert "Hello World" in (row["stdout"] or "")

    db.close()