from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Script:
    id: str
    name: str
    path: str
    description: Optional[str] = None
    hash: Optional[str] = None
    enabled: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Execution:
    id: str
    script_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "running"
    exit_code: Optional[int] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None