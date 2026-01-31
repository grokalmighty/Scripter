from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

class Trigger(ABC):
    def __init__(self, config: dict):
        self.config = config
    
    @abstractmethod
    def should_trigger(self, last_run: datetime | None) -> bool:
        """
        Return True if the job should run now.
        """
        raise NotImplementedError
    
    @abstractmethod 
    def next_run(self, after: datetime) -> datetime:
        """
        Return the next time this trigger should fire after the given time.
        """
        raise NotImplementedError