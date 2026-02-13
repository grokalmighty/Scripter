from __future__ import annotations
from pydantic import BaseModel
from typing import Optional

class Script(BaseModel):
    id: int
    name: str
    command: str
    working_dir: Optional[str] = None
    created_at: str
    updated_at: str
    