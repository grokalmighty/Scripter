from __future__ import annotations 

from datetime import datetime, time as dtime, timedelta
from .base import Trigger

class TimeTrigger(Trigger):
    def __init__(self, config: dict):
        super().__init__(config)
        self.time = self._parse_time(config["time"])
        self.days = [d.lower() for d in config.get("days", [])] or None

    def should_trigger(self, last_run: datetime | None) -> bool:
        now = datetime.now()

        if self.days and now.strftime("%A").lower() not in self.days:
            return False
        
        if now.time() < self.time:
            return False
        
        if last_run and last_run.date() == now.date():
            return False
        
        return True
    
    def next_run(self, after: datetime) -> datetime:
        candidate = datetime.combine(after.date(), self.time)

        if candidate <= after:
            candidate += timedelta(days=1)

        if self.days:
            while candidate.strftime("%A").lower() not in self.days:
                candidate += timedelta(days=1)
        
        return candidate
    
    @staticmethod
    def _parse_time(s: str) -> dtime:
        hh, mm = s.split(":")
        return dtime(hour=int(hh), minute=int(mm))