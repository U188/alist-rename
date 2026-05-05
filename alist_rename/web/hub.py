"""In-memory log hub and event normalization."""
from __future__ import annotations

import os
import queue
import re
import threading
import time
from dataclasses import dataclass, asdict
from typing import Dict, List


def _cn2int(s: str) -> int:
    table = {"零":0,"一":1,"二":2,"两":2,"三":3,"四":4,"五":5,"六":6,"七":7,"八":8,"九":9}
    s = str(s or "").strip()
    if s.isdigit():
        return int(s)
    if s == "十":
        return 10
    if "十" in s:
        left, _, right = s.partition("十")
        a = table.get(left, 1) if left else 1
        b = table.get(right, 0) if right else 0
        return a * 10 + b
    val = 0
    for ch in s:
        if ch in table:
            val = val * 10 + table[ch]
    if val <= 0:
        raise ValueError(f"cannot parse: {s}")
    return val

@dataclass
class LogEvent:
    id: int
    ts: float
    level: str        # INFO/DRY/SKIP/ERROR/...
    action: str       # rename/move/skip/error/...
    # Optional structured fields (can be empty for generic log lines)
    show: str = ""
    season: str = ""
    message: str = ""
    src: str = ""
    dst: str = ""

class LogHub:
    """Collect logs, write to file, and feed the web UI.

    API expected by renamer.py:
      - LogHub(log_file=..., also_print=True, keep=N)
      - emit(level, message)
      - close()
      - subscribe()/snapshot()/stats() for the UI
    """

    def __init__(self, log_file: str = '', also_print: bool = True, keep: int = 500):
        self.log_file = log_file
        self.latest_log_file = ''
        self.also_print = also_print
        self.keep = max(1, int(keep))
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self.running = False
        self._events: List[LogEvent] = []
        self._seq: int = 0  # monotonically increasing event id
        self._subscribers: List[queue.Queue] = []
        self._counts_level: Dict[str, int] = {}
        self._counts_action: Dict[str, int] = {}
        # lightweight "context" so the UI can group logs by series
        self._ctx_show: str = ""
        self._ctx_season: str = ""
        self._fh = None
        if self.log_file:
            self.set_log_file(self.log_file)

    def set_latest_log_file(self, latest_log_file: str) -> None:
        """Set an optional stable file that mirrors every emitted log line."""
        self.latest_log_file = latest_log_file or ''
        if self.latest_log_file:
            os.makedirs(os.path.dirname(self.latest_log_file) or '.', exist_ok=True)

    def set_log_file(self, log_file: str) -> None:
        """Switch the persistent log file used by this hub.

        The WebUI keeps one long-lived LogHub instance for SSE/in-memory events, while each
        clicked run should still have its own disk log.  This method lets the WebUI attach a
        fresh run log before starting a worker, without losing browser subscribers.
        """
        old_fh = self._fh
        self.log_file = log_file or ''
        self._fh = None
        if self.log_file:
            os.makedirs(os.path.dirname(self.log_file) or '.', exist_ok=True)
            self._fh = open(self.log_file, 'a', encoding='utf-8', buffering=1)
        if old_fh:
            try:
                old_fh.close()
            except Exception:
                pass

    def _next_id(self) -> int:
        """Return a monotonically increasing event id (thread-safe)."""
        with self._lock:
            self._seq += 1
            return self._seq



    def _infer_season(self, text: str) -> str:
        if not text:
            return ""
        m = re.search(r"(?i)\bS(\d{1,2})\b", text)
        if m:
            try:
                return f"S{int(m.group(1)):02d}"
            except Exception:
                return ""
        # 第X季
        m = re.search(r"第\s*([一二三四五六七八九十\d]{1,3})\s*季", text)
        if m:
            raw = m.group(1)
            try:
                return f"S{_cn2int(raw):02d}"
            except Exception:
                return ""
        return ""

    def _infer_structured_fields(self, message: str) -> Dict[str, str]:
        """Best-effort parse of show/season/src/dst from a plain log line."""
        msg = message or ""

        # Context: === PROCESS: /path/to/series ===
        m = re.search(r"===\s*PROCESS:\s*(.+?)\s*===", msg)
        if m:
            p = m.group(1).strip().rstrip("/")
            show = os.path.basename(p)
            self._ctx_show = show
            self._ctx_season = ""
            return {"show": show, "season": "", "src": p, "dst": ""}

        show = self._ctx_show
        season = self._infer_season(msg) or self._ctx_season

        # If we can detect a season on this line, keep it in context.
        if season and season != self._ctx_season:
            self._ctx_season = season

        src = ""
        dst = ""

        # rename /path/file.ext -> newname.ext
        m = re.search(r"\brename\s+(?P<src>.+?)\s*->\s*(?P<dst>.+)$", msg)
        if m:
            src = m.group("src").strip()
            dst = m.group("dst").strip()

        # move [name] : src_dir -> dst_dir
        m = re.search(r"\bmove\s+\[(?P<name>.+?)\]\s*:\s*(?P<src>.+?)\s*->\s*(?P<dst>.+)$", msg)
        if m:
            name = m.group("name").strip()
            src_dir = m.group("src").strip()
            dst_dir = m.group("dst").strip()
            src = f"{src_dir.rstrip('/')}/{name}" if src_dir else name
            dst = f"{dst_dir.rstrip('/')}/{name}" if dst_dir else name

        # mkdir /path
        m = re.search(r"\bmkdir\s+(?P<dst>.+)$", msg)
        if m and not dst:
            dst = m.group("dst").strip()

        return {"show": show, "season": season, "src": src, "dst": dst}

    def _infer_action(self, msg: str) -> str:
        m = msg.lower()
        if '[dry]' in m and 'rename' in m:
            return 'rename'
        if '[dry]' in m and 'move' in m:
            return 'move'
        if 'rename ' in m:
            return 'rename'
        if ' move ' in m or m.startswith('move '):
            return 'move'
        if '[skip]' in m:
            return 'skip'
        if '[error]' in m:
            return 'error'
        return ''

    def emit(self, level: str, message: str) -> None:
        ts = _now_ts()
        line = f"{ts} | {level:<6} | {message}"
        if self._fh:
            try:
                self._fh.write(line + "\n")
            except Exception:
                pass
        if self.latest_log_file:
            try:
                with open(self.latest_log_file, 'a', encoding='utf-8') as fh:
                    fh.write(line + "\n")
            except Exception:
                pass
        if self.also_print:
            try:
                print(line)
            except Exception:
                pass
        fields = self._infer_structured_fields(message)
        ev = LogEvent(
            id=self._next_id(),
            ts=ts,
            level=level,
            message=message,
            action=self._infer_action(message),
            show=fields.get("show", ""),
            season=fields.get("season", ""),
            src=fields.get("src", ""),
            dst=fields.get("dst", ""),
        )
        self.push(ev)

    def push(self, ev: LogEvent) -> None:
        with self._lock:
            self._events.append(ev)
            if len(self._events) > self.keep:
                self._events = self._events[-self.keep :]
            self._counts_level[ev.level] = self._counts_level.get(ev.level, 0) + 1
            if ev.action:
                self._counts_action[ev.action] = self._counts_action.get(ev.action, 0) + 1
            for q in list(self._subscribers):
                try:
                    q.put_nowait(ev)
                except Exception:
                    pass

    def snapshot(self, limit: int = 2000, since: int = 0) -> List[LogEvent]:
        with self._lock:
            if since and since > 0:
                evs = [e for e in self._events if e.id > since]
                return evs[-limit:]
            return list(self._events[-limit:])

    def stats(self) -> Dict[str, object]:
        with self._lock:
            return {
                "counts_level": dict(self._counts_level),
                "counts_action": dict(self._counts_action),
                "total": len(self._events),
                "running": bool(getattr(self, "running", False)),
                "stop_requested": self.stop_requested(),
            }

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue()
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def request_stop(self) -> None:
        """Request the running job to stop gracefully.

        The renamer loop checks `hub.stop_requested()` periodically.
        """
        try:
            self._stop_event.set()
            self.emit("WARN", "[STOP] stop requested")
        except Exception:
            self._stop_event.set()

    def stop_requested(self) -> bool:
        return bool(self._stop_event.is_set())

    def set_runtime_hooks(self, on_run=None, on_reload=None, stop_event=None, config_store=None, runtime_getter=None):
        self._on_run = on_run
        self._on_reload = on_reload
        self._runtime_getter = runtime_getter
        self._config_store = config_store
        if stop_event is not None:
            self._stop_event = stop_event

    def get_runtime_hooks(self):
        return {
            'on_run': getattr(self, '_on_run', None),
            'on_reload': getattr(self, '_on_reload', None),
            'runtime_getter': getattr(self, '_runtime_getter', None),
            'config_store': getattr(self, '_config_store', None),
        }

    def close(self):
        if self._fh:
            try:
                self._fh.close()
            except Exception:
                pass

def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")
