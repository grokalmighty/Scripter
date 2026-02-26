from __future__ import annotations

import json
from typing import List

from .base import TriggerSource
from ..database import Database
from ..triggers.base import TriggerEvent
from ..pending_events_repo import claim_ready_events

class InternalQueueSource(TriggerSource):
    def __init__(self, owner: str) -> None:
        self._owner = owner

    def poll(self, db: Database) -> List[TriggerEvent]:
        claimed = claim_ready_events(db, owner=self._owner)

        events: List[TriggerEvent] = []
        for row in claimed:
            payload = {}
            if row.get("payload_json"):
                try:
                    payload = json.loads(row["payload_json"])
                except Exception:
                    payload = {"payload_json": row["payload_json"]}
            
            payload["_pending_id"] = int(row["id"])

            events.append(
                TriggerEvent(
                    trigger_id=row["trigger_id"],
                    script_id=int(row["script_id"]),
                    payload=payload,
                )
            )
        return events
