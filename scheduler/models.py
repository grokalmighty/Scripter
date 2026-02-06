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