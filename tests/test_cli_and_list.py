import os
import subprocess
import sys
from pathlib import Path

def run_cmd(args, env):
    return subprocess.run(
        [sys.executable, "-m", "scheduler", *args],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )

def test_cli_add_then_list(tmp_path):
    fake_home = tmp_path / "home"
    fake_home.mkdir()

    env = os.environ.copy()
    env["HOME"] = str(fake_home)

    run_cmd(["add", "hello", "/tmp/hello.sh"], env=env)
    result = run_cmd(["list"], env=env)

    assert "hello -> /tmp/hello.sh" in result.stdout

    db_file = fake_home / ".scheduler" / "scheduler.db"
    assert db_file.exists()