from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Optional

@dataclass
class ExecResult:
    exit_code: int
    stdout: str
    stderr: str

def run_command(command: str, working_dir: Optional[str] = None, timeout: int = 60) -> ExecResult:
    proc = subprocess.run(
        command, 
        shell=True,
        cwd=working_dir,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return ExecResult(
        exit_code=proc.returncode,
        stdout=proc.stdout or "",
        stderr=proc.stderr or "",
    )