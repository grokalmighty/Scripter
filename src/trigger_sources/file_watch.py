from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List

from .base import TriggerSource
from ..database import Database
from ..file_triggers_repo import list_file_triggers
from ..file_watcher import FileWatcher
from ..triggers.base import TriggerEvent

class FileWatchSource(TriggerSource):
    """
    Polls file triggers using the existing FileWatcher.
    """

    QUIET_SECONDS = 3
    MIN_INTERVAL_SECONDS = 30

    def __init__(self) -> None:
        self._watcher = FileWatcher()
        self._last_change_seen: Dict[int, datetime] = {}
        self._last_executed_for_change: Dict[int, datetime] = {}
        self._last_exec_time: Dict[int, datetime] = {}
    
    def poll(self, db: Database) -> List[TriggerEvent]:
        events: List[TriggerEvent] = []
        now = datetime.now(timezone.utc)

        for ft in list_file_triggers(db):
            ft_id = int(ft["id"])
            script_id = int(ft["script_id"])
            path = ft["path"]
            recursive = bool(ft["recursive"])

            try:
                changed = self._watcher.scan(path, recursive)
            except Exception:
                continue 
            if changed:
                self._last_change_seen[ft_id] = now
                continue
                
            last_change = self._last_change_seen.get(ft_id)
            if last_change is None:
                continue

            if (now - last_change).total_seconds() < self.QUIET_SECONDS:
                continue

            last_exec_for_change = self._last_executed_for_change.get(ft_id)
            if last_exec_for_change is not None and last_exec_for_change >= last_change:
                continue

            last_time = self._last_exec_time.get(ft_id)
            if last_time is not None and (now - last_time).total_seconds() < self.MIN_INTERVAL_SECONDS:
                continue

            self._last_exec_time[ft_id] = now
            self._last_executed_for_change[ft_id] = now

            events.append(
                TriggerEvent(
                    trigger_id=f"file:{ft_id}",
                    script_id=script_id,
                    payload={
                        "file_trigger_id": ft_id,
                        "path": path,
                        "recusirve": recursive,
                    },
                )
            ) 
        
        return events