from __future__ import annotations

from threading import Thread, Event
from typing import Optional

from .database import Database
from .executor import ScriptExecutor
from .triggers.time import TimeTrigger

class Scheduler:
    def __init__(self, db: Database, executor: ScriptExecutor, tick_seconds: int = 30):
        self.db = db
        self.executor = executor
        self.tick_seconds = tick_seconds
        self._stop_event = Event()
        self._thread: Optional[Thread] = None
    
    def start(self) -> None:
        if self._thread and self._thread._is_alive():
            return 
        self._stop_event.clear()
        self._thread = Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
    
    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            scripts = self.db.get_enabled_scripts()

            for script in scripts:
                schedules = self.db.get_schedules(script.id)
                last_run = self.db.get_last_execution_time(script.id)

                for sched in schedules:
                    trigger = self._create_trigger(sched["trigger_type"], sched["trigger_config"])
                    if trigger.should_trigger(last_run):
                        print(f"[scheduler] running {script.name}")
                        Thread(target=self.executor.execute, args=(script.id,), daemon=True).start()
            
            self._stop_event.wait(self.tick_seconds)
    
    def _create_trigger(self, trigger_type: str, trigger_config: dict):
        if trigger_type == "time":
            return TimeTrigger(trigger_config)
        raise ValueError(f"Unknown trigger type: {trigger_type}")