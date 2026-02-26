from __future__ import annotations

from typing import Dict, List, Set

from .base import TriggerSource
from ..database import Database
from ..triggers.base import TriggerEvent
from ..app_triggers_repo import list_app_triggers

try:
    import psutil
except Exception:  # pragma: no cover
    psutil = None


def _running_process_names() -> Set[str]:
    """
    Returns a lowercase set of process 'name's currently running.
    """
    names: Set[str] = set()
    if psutil is None:
        return names

    for p in psutil.process_iter(attrs=["name"]):
        try:
            n = p.info.get("name") or ""
            if n:
                names.add(n.lower())
        except Exception:
            continue
    return names


class AppWatchSource(TriggerSource):
    """
    Polls running processes and fires events on transitions:
      - app:launch:<process>
      - app:exit:<process>
    """

    def __init__(self) -> None:
        # last known running state per normalized process_name key
        self._last: Dict[str, bool] = {}

    def poll(self, db: Database) -> List[TriggerEvent]:
        triggers = list_app_triggers(db)
        running = _running_process_names()
        print("APPWATCH:", "triggers=", [(t.process_name, t.on_event, t.script_id) for t in triggers])
        print("APPWATCH:", "safari_running=", ("safari" in running))
        if not triggers:
            return []

        running = _running_process_names()

        # group triggers by process_name (normalized)
        by_proc: Dict[str, list] = {}
        for t in triggers:
            key = t.process_name.lower()
            by_proc.setdefault(key, []).append(t)

        events: List[TriggerEvent] = []

        for proc_key, group in by_proc.items():
            is_running = proc_key in running
            was_running = self._last.get(proc_key, False)

            # detect transitions
            if (not was_running) and is_running:
                # launched
                for t in group:
                    if t.on_event == "launch":
                        events.append(
                            TriggerEvent(
                                trigger_id=f"app:launch:{t.process_name}",
                                script_id=t.script_id,
                                payload={"process": t.process_name, "event": "launch"},
                            )
                        )

            if was_running and (not is_running):
                # exited
                for t in group:
                    if t.on_event == "exit":
                        events.append(
                            TriggerEvent(
                                trigger_id=f"app:exit:{t.process_name}",
                                script_id=t.script_id,
                                payload={"process": t.process_name, "event": "exit"},
                            )
                        )

            self._last[proc_key] = is_running

        return events