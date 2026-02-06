from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Optional, Sequence

from .database import Database

class ScriptNotFoundError(Exception):
    pass

@dataclass
class ExecResult:
    returncode: int
    stdout: str
    stderr: str

class ScriptExecutor:
    def __init__(self, db: Database):
        self.db = db

    def execute(self, script_id: str, timeout: Optional[int] = None) -> ExecResult:
        script = self.db.get_script(script_id)

        if script is None:
            raise ScriptNotFoundError(f"Script with ID '{script_id}' not found.")
        
        execution_id = self.db.create_execution(script_id)

        try:
            result = subprocess.run(
                [script.path],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            status = "success" if result.returncode == 0 else "failed"

            self.db.complete_execution(
                execution_id,
                status=status,
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )

            return ExecResult(result.returncode, result.stdout, result.stderr)
        
        except subprocess.TimeoutExpired as e:
            self.db.complete_execution(
                execution_id,
                status="timeout",
                exit_code=None,
                stdout=e.stdour if isinstance(e.stdout, str) else (e.stdout.decode() if e.stdout else ""),
                stderr=e.stderr if isinstance(e.stderr, str) else (e.stderr.decode() if e.stderr else ""),
            )
            raise