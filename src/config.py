from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Any
import yaml

@dataclass(frozen=True)
class Settings:
    db_path: Path = Path.cwd() / "scripter.db"
    tick_seconds: int = 2

    file_quiet_seconds: int = 3
    file_min_interval_seconds: int = 30

    webhook_host: str = "127.0.0.1"
    webhook_port: int = 5055

def load_settings(path: Optional[Path]) -> Settings:
    """
    Load settings from a YAML file. Missing values fall back to defaults.
    """

    if path is None:
        return Settings()
    
    data: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    s = data.get("settings", {})

    return Settings(
        db_path=Path(s.get("db_path", Settings().db_path)),
        tick_seconds=int(s.get("tick_seconds", Settings().tick_seconds)),
        file_quiet_seconds=int(s.get("file_quiet_seconds", Settings().file_quiet_seconds)),
        file_min_interval_seconds=int(s.get("file_min_interval_seconds", Settings().file_min_interval_seconds)),
        webhook_host=str(s.get("webhook_host", Settings().webhook_host)),
        webhook_port=int(s.get("webhook_port", Settings().webhook_port)),
    )