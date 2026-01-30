from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .database import Database
from .models import Script

class SecurityError(RuntimeError):
    pass

def calculate_hash(file_path: str) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

@dataclass
class ExecResult:
    returncode: int
    stdout: str
    stderr: str
    execution_id: str

class ScriptExecutor:
    def __init__(self, db: Database):
        self.db = db

    def execute(self, script_id: str, timeout: Optional[int] = None) -> ExecResult:
        script = self.db.get_script(script_id)
        if not script:
            raise ValueError(f"Unknown script id: {script_id}")
        
        script_path = Path(script.path)

        if not script_path.exists():
            raise FileNotFoundError(f"Script not found: {script.path}")
        
        if script.hash:
            current = calculate_hash(script.path)
            if current != script.hash:
                raise SecurityError(f"Hash mismatch for script '{script.name}'")
            
        execution_id = self.db.create_execution(script_id)

        try:
            result = subprocess.run(
                [script.path],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            self.db.complete_execution(
                execution_id,
                exit_code=result.returncode,
                stdout=result.stdout or "",
                stderr=result.stderr or "",
                status="completed",
            )
            return ExecResult(
                returncode=result.returncode,
                stdout=result.stdout or "",
                stderr=result.stderr or "",
                execution_id=execution_id,
            )
        except subprocess.TimeoutExpired as e:
            self.db.complete_execution(
                execution_id,
                exit_code=124,
                stdout=(e.stdout or "") if isinstance(e.stdout, str) else "",
                stderr=(e.stderr or "") if isinstance(e.stderr, str) else "",
                status="timeout",
            )
            raise