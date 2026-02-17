from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from .base import TriggerSource
from ..database import Database
from ..triggers.base import TriggerEvent
from ..oneshots_repo import claim_due_one_shots

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

class OneShotSource(TriggerSource):
    def poll(self, db: Database) -> List[TriggerEvent]:
        now = _utc_now_iso()
        claimed = claim_due_one_shots(db, now)

        events: List[TriggerEvent] = []
        for row in claimed:
            oneshot_id = int(row["id"])
            events.append(
                TriggerEvent(
                    trigger_id=f"oneshot:{oneshot_id}",
                    script_id=int(row["script_id"]),
                    payload={"run_at_utc": row["run_at_utc"], "tz": row.get("tz")},
                )
            )
        return events
