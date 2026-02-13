from __future__ import annotations

from datetime import datetime

def to_local_display(iso_ts: str | None) -> str:
    if not iso_ts:
        return ""
    dt = datetime.fromisoformat(iso_ts)
    return dt.astimezone().strftime("%Y-%m-%d %I:%M:%S %p %Z")