import subprocess
import sys

def test_cli_smoke():
    result = subprocess.run(
        [sys.executable, "-m", "scheduler", "hello"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "hello from scheduler" in result.stdout