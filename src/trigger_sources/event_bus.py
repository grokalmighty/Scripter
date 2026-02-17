from __future__ import annotations

from typing import List

from .base import TriggerSource
from ..database import Database
from ..triggers.base import TriggerEvent
from ..event_bus_repo import claim_ready_deliveries

class EventBusSource(TriggerSource):
    def __init__(self, owner: str) -> None:
        self._owner = owner

    def poll(self, db: Database) -> List[TriggerEvent]:
        claimed = claim_ready_deliveries(db, owner=self._owner)

        events: List[TriggerEvent] = []
        for row in claimed:
            topic = row["topic"]
            delivery_id = int(row["delivery_id"])
            event_id = int(row["event_id"])
            script_id=int(row["script_id"])

            events.append(
                TriggerEvent(
                    trigger_id=f"event:{topic}",
                    script_id=script_id,
                    payload={
                        "topic": topic,
                        "event_id": event_id,
                        "delivery_id": delivery_id,
                        "payload_json": row.get("payload_json"),
                    },
                )
            )
        return events