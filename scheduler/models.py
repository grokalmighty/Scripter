from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime

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
class Schedule:
    id: str
    script_id: str
    trigger_type: str
    trigger_config: Dict[str, Any]

@dataclass
class Execution:
    id: str
    script_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "running"
    exit_code: Optional[int] = None
    