from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

@dataclass(frozen=True)
class TriggerEvent:
    """
    A normalized trigger emission.
    
    trigger_id: stable namespaced id
    payload: optional structured data for debug
    """
    trigger_id: str
    script_id: int
    payload: Dict[str, Any] = field(default_factory=dict)
