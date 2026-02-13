from __future__ import annotations

from pathlib import Path
from typing import Dict
import os 

class FileWatcher:
    def __init__(self):
        self._state: Dict[str, Dict[str, float]] = {}
    
    def scan(self, base_path: str, recursive: bool) -> bool:
        """
        Returns True if any file changed since last scan.
        """
        base = Path(base_path)

        if not base.exists():
            return False
        
        files = []
        if recursive:
            for root, _, filenames in os.walk(base):
                for f in filenames:
                    files.append(Path(root) / f)
        else:
            files = [p for p in base.iterdir() if p.is_file()]
        
        current = {str(p): p.stat().st_mtime for p in files}

        previous = self._state.get(base_path, {})
        
        changed = False

        for path, mtime in current.items():
            if path not in previous or previous[path] != mtime:
                changed = True
                break

        self._state[base_path] = current
        return changed 