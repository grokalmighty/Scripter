from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from src.database import Database
from src.triggers.base import TriggerEvent

class TriggerSource(ABC):
    """
    A source of TriggerEvents polled by scheduler.
    """

    @abstractmethod
    def poll(self, db: Database) -> List[TriggerEvent]:
        raise NotImplementedError