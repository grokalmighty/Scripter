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
from .trigger_sources.internal_queue import InternalQueueSource
from .pending_events_repo import mark_processed

import signal
from dataclasses import dataclass
from .pending_events_repo import enqueue_event
from .daemon_hooks_repo import hooks_for_event
from .signal_hooks_repo import hooks_for_signal

@dataclass
class _DaemonControl:
    stop: bool = False
    reload: bool = False
    last_signal: str | None = None

def _enqueue_daemon_event(db: Database, event: str):
    for script_id in hooks_for_event(db, event):
        enqueue_event(
            db,
            trigger_id=f"daemon:{event}",
            script_id=script_id,
            payload={"event": event},
        )

def _enqueue_signal_event(db: Database, sig: str):
    for script_id in hooks_for_signal(db, sig):
        enqueue_event(
            db,
            trigger_id=f"signal:{sig}",
            script_id=script_id,
            payload={"signal": sig},
        )
def run_loop(db_path: Optional[Path] = None, 
             tick_seconds: int = 2, 
             once: bool = False,
             sources: Optional[Iterable[TriggerSource]] = None,) -> None:
    db = Database(db_path)
    db.init()

    owner = owner_id()

    ctl = _DaemonControl()

    def _handle(sig_num, _frame):
        try:
            name = signal.Signals(sig_num).name
        except Exception:
            name = f"SIG{sig_num}"
        short = name.replace("SIG", "")

        ctl.last_signal = short
        _enqueue_signal_event(db, short)

        if short in ("INT", "TERM"):
            ctl.stop = True
        elif short == "HUP":
            ctl.reload = True
    
    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, _handle)

    _enqueue_daemon_event(db, "start")
    
    active_sources: list[TriggerSource] = list(
        sources if sources is not None else [
                                            ScheduleSource(), 
                                            OneShotSource(), 
                                            EventBusSource(owner),
                                            InternalQueueSource(owner),
                                            FileWatchSource()]
    )

    while not ctl.stop:
        if ctl.reload:
            ctl.reload = False
            _enqueue_daemon_event(db, "reload")

        for source in active_sources:
            events = source.poll(db) or []
            for event in events:
                pending_id = event.payload.get("_pending_id")

                def on_finished(status, run_id):
                    if pending_id is not None:
                        mark_processed(db, int(pending_id))
                    delivery_id = event.payload.get("delivery_id")
                    if delivery_id is not None:
                        mark_delivery_processed(db, int(delivery_id))

                execute_event(db, event, owner, on_finished=on_finished)

        if once:
            return

        for _ in range(max(1, tick_seconds * 10)):
            if ctl.stop:
                break
            time.sleep(0.1)

    _enqueue_daemon_event(db, "stop")
    for source in active_sources:
        events = source.poll(db) or []
        for event in events:
            pending_id = event.payload.get("_pending_id")

            def on_finished(status, run_id):
                if pending_id is not None:
                    mark_processed(db, int(pending_id))
                delivery_id = event.payload.get("delivery_id")
                if delivery_id is not None:
                    mark_delivery_processed(db, int(delivery_id))

            execute_event(db, event, owner, on_finished=on_finished)