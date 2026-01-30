import os
from pathlib import Path

from scheduler.database import Database
from scheduler.executor import ScriptExecutor, calculate_hash
from scheduler.models import Script

def test_execute_script_records_output(tmp_path: Path):
    script_path = tmp_path / "test.sh"
    script_path.write_text("#!/bin/bash\necho 'Hello World'\necho 'Err here' 1>&2\n")
    script_path.chmod(0o755)

    db = Database(":memory:")
    db.connect()

    s = Script(
        id="s1",
        name="hello",
        path=str(script_path),
        hash=calculate_hash(str(script_path)),
    )
    db.add_script(s)

    executor = ScriptExecutor(db)
    res = executor.execute("s1")

    assert res.returncode == 0
    assert "Hello World" in res.stdout
    assert "Err here" in res.stderr

    row = db.get_execution(res.execution_id)
    assert row is not None
    assert row["exit_code"] == 0
    assert "Hello World" in (row["stdout"] or "")
    assert "Err here" in (row["stderr"] or "")