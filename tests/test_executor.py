from src.executor import run_command

def test_run_command_echo():
    result = run_command("echo hi")

    assert result.exit_code == 0
    assert "hi" in result.stdout.strip()
    assert result.stderr == ""