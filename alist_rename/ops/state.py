"""State and undo log persistence."""
from __future__ import annotations

import json
import os
import threading

from alist_rename.common.paths import norm_path

class UndoLogger:
    """Append-only undo log in JSONL.

    Records operations in APPLY mode so you can rollback if needed.
    """

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()

    def record(self, obj: dict):
        if not self.path:
            return
        line = json.dumps(obj, ensure_ascii=False)
        with self._lock:
            with open(self.path, 'a', encoding='utf-8') as f:
                f.write(line + '\n')

def load_state(path: str) -> set:
    done=set()
    if not path or not os.path.exists(path):
        return done
    try:
        with open(path,'r',encoding='utf-8') as f:
            for line in f:
                line=line.strip()
                if not line:
                    continue
                try:
                    o=json.loads(line)
                    if o.get('status')=='done' and o.get('series_path'):
                        done.add(norm_path(o['series_path']))
                except Exception:
                    continue
    except Exception:
        pass
    return done

def append_state(path: str, obj: dict):
    if not path:
        return
    try:
        with open(path,'a',encoding='utf-8') as f:
            f.write(json.dumps(obj, ensure_ascii=False)+'\n')
    except Exception:
        pass

__all__ = ["UndoLogger", "load_state", "append_state"]
