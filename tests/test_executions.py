from scheduler.database import Database
from scheduler.models import Script

def test_create_and_complete_execution():
    db = Database(":memory:")
    db.connect()

    script = Script(id="s1", name="hello", path="/tmp/hello.sh")
    db.add_script(script)

    execution_id = db.create_execution("s1")

    row = db.get_last_execution_row("s1")
    assert row is not None
    assert row["id"] == execution_id
    assert row["status"] == "running"
    assert row["completed_at"] is None
    
    db.complete_execution(
        execution_id,
        status="success",
        exit_code=0,
        stdout="hi\n",
        stderr="",
    )

    row2 = db.get_last_execution_row("s1")
    assert row2["status"] == "success"
    assert row2["exit_code"] == 0
    assert row2["stdout"] == "hi\n"
    assert row2["completed_at"] is not None

    db.close()