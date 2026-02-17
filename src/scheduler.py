from __future__ import annotations

import time
from pathlib import Path
from typing import Optional, Iterable

from .database import Database
from .event_bus_repo import mark_delivery_processed
from .locks import owner_id
from .run_service import execute_event
from .trigger_sources.base import TriggerSource
from .trigger_sources.event_bus import EventBusSource
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
        sources if sources is not None else [ScheduleSource(), OneShotSource(), EventBusSource(owner), FileWatchSource()]
    )

    while True:
        for source in active_sources:
            events = source.poll(db) or []
            for event in events:
                def on_finished(status, run_id):
                    delivery_id = event.payload.get("delivery_id")
                    if delivery_id is not None:
                        mark_delivery_processed(db, int(delivery_id))
                execute_event(db, event, owner, on_finished=on_finished)
        if once:
            return
        time.sleep(tick_seconds)