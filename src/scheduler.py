from __future__ import annotations

import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from .database import Database
from .daemon_hooks_repo import hooks_for_event
from .event_bus_repo import mark_delivery_processed
from .locks import owner_id
from .pending_events_repo import mark_processed
from .run_service import execute_event
from .runs_repo import is_script_running
from .scripts_repo import get_script
from .signal_hooks_repo import hooks_for_signal
from .trigger_sources.app_watch import AppWatchSource
from .trigger_sources.base import TriggerSource
from .trigger_sources.event_bus import EventBusSource
from .trigger_sources.file_watch import FileWatchSource
from .trigger_sources.internal_queue import InternalQueueSource
from .trigger_sources.one_shots import OneShotSource
from .trigger_sources.schedules import ScheduleSource


@dataclass
class _DaemonControl:
    stop: bool = False
    reload: bool = False
    last_signal: str | None = None


def run_loop(
    db_path: Optional[Path] = None,
    tick_seconds: int = 2,
    once: bool = False,
    sources: Optional[Iterable[TriggerSource]] = None,
) -> None:
    from .pending_events_repo import enqueue_event

    db = Database(db_path)
    db.init()

    owner = owner_id()

    # Single-daemon guard (DB-backed)
    from .daemon_lock_repo import acquire_daemon_lock, release_daemon_lock

    acquire_daemon_lock(db, owner)

    ctl = _DaemonControl()

    def _enqueue_daemon_event(event: str) -> None:
        for script_id in hooks_for_event(db, event):
            enqueue_event(
                db,
                trigger_id=f"daemon:{event}",
                script_id=script_id,
                payload={"event": event},
            )

    def _enqueue_signal_event(sig: str) -> None:
        for script_id in hooks_for_signal(db, sig):
            enqueue_event(
                db,
                trigger_id=f"signal:{sig}",
                script_id=script_id,
                payload={"signal": sig},
            )

    def _handle(sig_num, _frame):
        try:
            name = signal.Signals(sig_num).name
        except Exception:
            name = f"SIG{sig_num}"
        short = name.replace("SIG", "")

        ctl.last_signal = short
        _enqueue_signal_event(short)

        if short in ("INT", "TERM"):
            ctl.stop = True
        elif short == "HUP":
            ctl.reload = True

    # Register signal handlers
    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, _handle)

    # Build sources
    active_sources: list[TriggerSource] = list(
        sources
        if sources is not None
        else [
            ScheduleSource(),
            OneShotSource(),
            EventBusSource(owner),
            AppWatchSource(),
            InternalQueueSource(owner),
            FileWatchSource(),
        ]
    )

    try:
        _enqueue_daemon_event("start")

        while not ctl.stop:
            if ctl.reload:
                ctl.reload = False
                _enqueue_daemon_event("reload")

            for source in active_sources:
                events = source.poll(db) or []
                for event in events:
                    script = get_script(db, event.script_id)
                    policy = getattr(script, "concurrency_policy", "allow") or "allow"

                    # --- Concurrency policies ---
                    if policy == "skip":
                        if is_script_running(db, event.script_id):
                            pending_id = event.payload.get("_pending_id")
                            if pending_id is not None:
                                mark_processed(db, int(pending_id))
                            continue

                    elif policy == "queue_one":
                        if is_script_running(db, event.script_id):
                            pending_id = event.payload.get("_pending_id")

                            # If this event came from pending_events and script is still running,
                            # keep at most one waiting row and unclaim/mark accordingly.
                            if pending_id is not None:
                                from .pending_events_repo import has_other_pending_event, unclaim_event

                                if has_other_pending_event(db, event.script_id, exclude_id=int(pending_id)):
                                    mark_processed(db, int(pending_id))
                                else:
                                    unclaim_event(db, int(pending_id))
                                continue

                            # Event came from a "real" source while script is running:
                            # atomically enqueue at most one waiting pending_events row.
                            from .pending_events_repo import enqueue_queue_one, trim_queue_one

                            enqueue_queue_one(
                                db,
                                trigger_id=event.trigger_id,
                                script_id=event.script_id,
                                payload=event.payload,
                            )
                            continue

                    # Normal execution path
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

            # Sleep in small increments so SIGINT/SIGTERM can break quickly
            for _ in range(max(1, tick_seconds * 10)):
                if ctl.stop:
                    break
                time.sleep(0.1)

        # Graceful shutdown: enqueue stop hook and do one final poll/flush pass
        _enqueue_daemon_event("stop")
        for source in active_sources:
            events = source.poll(db) or []
            for event in events:
                script = get_script(db, event.script_id)
                policy = getattr(script, "concurrency_policy", "allow") or "allow"

                if policy == "skip":
                    if is_script_running(db, event.script_id):
                        pending_id = event.payload.get("_pending_id")
                        if pending_id is not None:
                            mark_processed(db, int(pending_id))
                        continue

                elif policy == "queue_one":
                    if is_script_running(db, event.script_id):
                        pending_id = event.payload.get("_pending_id")

                        if pending_id is not None:
                            from .pending_events_repo import has_other_pending_event, unclaim_event

                            if has_other_pending_event(db, event.script_id, exclude_id=int(pending_id)):
                                mark_processed(db, int(pending_id))
                            else:
                                unclaim_event(db, int(pending_id))
                            continue

                        enqueue_queue_one(
                            db,
                            trigger_id=event.trigger_id,
                            script_id=event.script_id,
                            payload=event.payload,
                        )
                        continue

                pending_id = event.payload.get("_pending_id")

                def on_finished(status, run_id):
                    if pending_id is not None:
                        mark_processed(db, int(pending_id))
                    delivery_id = event.payload.get("delivery_id")
                    if delivery_id is not None:
                        mark_delivery_processed(db, int(delivery_id))

                execute_event(db, event, owner, on_finished=on_finished)

    finally:
        # Always release lock + close DB, even on Ctrl+C or exceptions.
        try:
            release_daemon_lock(db, owner)
        finally:
            db.close()