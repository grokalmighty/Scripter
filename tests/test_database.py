from scheduler.database import Database
from scheduler.models import Script

def test_add_and_get_script_by_id():
    db = Database(":memory:")
    db.connect()

    script = Script(
        id="test_1",
        name="test_script",
        path="/tmp/test.sh",
        description="a test srcipt",
        hash="abc123",
        enabled=True,
    )

    db.add_script(script)

    retrieved = db.get_script("test_1")
    assert retrieved is not None
    assert retrieved.id == "test_1"
    assert retrieved.name == "test_script"
    assert retrieved.path == "/tmp/test.sh"
    assert retrieved.enabled is True

    db.close()

def test_get_script_by_name():
    db = Database(":memory:")
    db.connect()

    db.add_script(Script(id="s1", name="hello", path="/tmp/hello.sh"))
    retrieved = db.get_script_by_name("hello")

    assert retrieved is not None
    assert retrieved.id == "s1"
    assert retrieved.name == "hello"
    assert retrieved.path == "/tmp/hello.sh"

    db.close()