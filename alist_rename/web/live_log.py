"""Threaded WebUI server wrapper."""
from __future__ import annotations

import re
import threading
from http.server import HTTPServer, ThreadingHTTPServer
from typing import Optional

from alist_rename.web.hub import LogHub
from alist_rename.web.handler import make_handler

_DEFAULT_PORT = 55255

class LiveLog:
    """Run log server in a background thread."""

    def __init__(self, hub: LogHub, host: str = '127.0.0.1', port: int = _DEFAULT_PORT, token: str = ''):
        self.hub = hub
        self.host = host
        self.port = port
        self.token = token or ''
        self._http: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        handler = make_handler(self.hub, self.token)
        self._http = ThreadingHTTPServer((self.host, self.port), handler)
        # If port=0 (ephemeral), update to the actual bound port
        self.port = int(self._http.server_address[1])
        t = threading.Thread(target=self._http.serve_forever, daemon=True)
        t.start()
        self._thread = t

    def stop(self):
        if self._http:
            try:
                self._http.shutdown()
            except Exception:
                pass

    # --- Compatibility helpers ---
    # renamer.py historically treats `log` as a list-like sink and calls
    # `log.append("...")`.  In the web-UI mode `log` is a LiveLog instance,
    # so we provide an `append()` method that forwards to LogHub.emit().
    def append(self, message: str) -> None:
        try:
            msg = "" if message is None else str(message)
        except Exception:
            msg = "<unprintable>"

        # Try to infer level from a leading tag like "[DRY]" / "[SKIP]" / "[ERROR]".
        level = "INFO"
        m = re.match(r"^\[(?P<tag>[A-Za-z]+)\]", msg.strip())
        if m:
            tag = m.group("tag").upper()
            if tag in {"INFO", "DRY", "SKIP", "ERROR", "WARN", "WARNING"}:
                level = "WARN" if tag == "WARNING" else tag
            elif tag == "AI":
                level = "INFO"

        self.hub.emit(level, msg)

    def extend(self, items) -> None:
        for it in items or []:
            self.append(it)

def start_log_server(hub: LogHub, host: str = '127.0.0.1', port: int = _DEFAULT_PORT, token: str = None) -> LiveLog:
    """Start the log web UI (non-blocking)."""
    srv = LiveLog(hub=hub, host=host, port=port, token=token or '')
    srv.start()
    return srv
