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
            self._state.pop(base_path, None)
            return False
        
        files: list[Path] = []

        if base.is_file():
            file = [base]

        elif base.is_dir():
            if recursive:
                for root, _, filenames in os.walk(base):
                    for name in filenames:
                        files.append(Path(root) / name)
            else:
                files = [p for p in base.iterdir() if p.is_file()]
        else:
            return False
        

        current: Dict[str, float] = {}
        for p in files:
            try:
                current[str(p)] = p.stat().st_mtime
            except OSError:
                continue

        previous = self._state.get(base_path)
        
        if previous is None:
            self._state[base_path] = current
            return False
        
        if current.keys() != previous.keys():
            self._state[base_path] = current
            return True
        
        for path_str, mtime in current.items():
            if previous.get(path_str) != mtime:
                self._state[base_path] = current
                return True

        self._state[base_path] = current
        return False