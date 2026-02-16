from __future__ import annotations

from typing import List

from .base import TriggerSource
from ..database import Database
from ..schedules_repo import due_schedules, mark_run
from ..triggers.base import TriggerEvent

class ScheduleSource(TriggerSource):
    def poll(self, db: Database) -> List[TriggerEvent]:
        events: List[TriggerEvent] = []

        for sched in due_schedules(db):
            schedule_id = int(sched["id"])
            script_id = int(sched["script_id"])

            mark_run(db, schedule_id)

            events.append(
                TriggerEvent(
                    trigger_id=f"schedule:{schedule_id}",
                    script_id=script_id,
                    payload={"schedule_id": schedule_id},
                )
            )
        return events 