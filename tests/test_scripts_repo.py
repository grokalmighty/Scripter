from pathlib import Path
from src.database import Database
from src.scripts_repo import add_script, list_scripts, get_script

def test_add_list_get_script(tmp_path: Path):
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    sid = add_script(db, name="hello", command="echo hello", working_dir=None)
    assert sid > 0

    scripts = list_scripts(db)
    assert len(scripts) == 1
    assert scripts[0].name == "hello"

    s = get_script(db, sid)
    assert s is not None
    assert s.command == "echo hello"