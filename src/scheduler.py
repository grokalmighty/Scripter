from __future__ import annotations

import time
from pathlib import Path
from typing import Optional, Iterable

from .database import Database
from .locks import owner_id
from .run_service import execute_event
from .trigger_sources.base import TriggerSource
from .trigger_sources.file_watch import FileWatchSource
from .trigger_sources.schedules import ScheduleSource
from .trigger_sources.one_shots import OneShotSource

def run_loop(db_path: Optional[Path] = None, 
             tick_seconds: int = 2, 
             once: bool = False,
             sources: Optional[Iterable[TriggerSource]] = None,) -> None:
    db = Database(db_path)
    db.init()

    owner = owner_id()
    
    active_sources: list[TriggerSource] = list(
        sources if sources is not None else [ScheduleSource(), OneShotSource(), FileWatchSource()]
    )

    while True:
        for source in active_sources:
            events = source.poll(db) or []
            for event in events:
                execute_event(db, event, owner)
    
        if once:
            return
        time.sleep(tick_seconds)