# -*- coding: utf-8 -*-
"""Lightweight log web UI (no third-party deps).

Features
- Server-Sent Events (SSE) realtime logs
- Two-column layout (filters+stats | grouped logs)
- Filter by SKIP/ERROR/INFO/DRY, show keyword, season
- Counters per action and level
- Collapsible groups by show
- Small charts (canvas) for action counts
- Optional token auth via URL query (?token=xxx) or header X-Token

This file intentionally avoids extra dependencies so it can run on a bare Ubuntu VPS.
"""

from __future__ import annotations

import json
import os
import queue
import re
import threading
import time
import logging
from dataclasses import dataclass, asdict
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Type
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from runtime_config import hash_password, verify_password

logger = logging.getLogger("embyrename.logui")

_DEFAULT_PORT = 55255


def _norm_path(path: str) -> str:
    path = (path or '').strip()
    if not path:
        return '/'
    if not path.startswith('/'):
        path = '/' + path
    path = re.sub(r'/+', '/', path)
    return path or '/'


def _get_alist_client_cls():
    from renamer import AlistClient
    return AlistClient


def _cn2int(s: str) -> int:
    """Convert a small set of Chinese numerals to int (1-99-ish).

    Supports: 一二三四五六七八九十, and mixed digits.
    """
    if not s:
        raise ValueError("empty")
    if s.isdigit():
        return int(s)
    table = {"零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
    if s == "十":
        return 10
    if "十" in s:
        left, _, right = s.partition("十")
        a = table.get(left, 1) if left else 1
        b = table.get(right, 0) if right else 0
        return a * 10 + b
    # simple additive (rare): 二三 -> 23 not intended; fall back to per char
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
            os.makedirs(os.path.dirname(self.log_file) or '.', exist_ok=True)
            self._fh = open(self.log_file, 'a', encoding='utf-8', buffering=1)

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


def _token_ok(handler: BaseHTTPRequestHandler, token: str) -> bool:
    if not token:
        return True
    # header
    t = handler.headers.get("X-Token") or ""
    if t == token:
        return True
    # query
    qs = parse_qs(urlparse(handler.path).query)
    return (qs.get("token") or [""])[0] == token


def _json_bytes(payload) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode('utf-8')

def _read_json(handler: BaseHTTPRequestHandler):
    try:
        length = int(handler.headers.get('Content-Length') or '0')
    except Exception:
        length = 0
    raw = handler.rfile.read(length) if length > 0 else b''
    if not raw:
        return {}
    try:
        return json.loads(raw.decode('utf-8'))
    except Exception:
        return {}

def _cookie_dict(handler: BaseHTTPRequestHandler) -> dict:
    c = cookies.SimpleCookie()
    try:
        c.load(handler.headers.get('Cookie') or '')
    except Exception:
        return {}
    return {k: morsel.value for k, morsel in c.items()}

def _session_sig(secret: str, raw: str) -> str:
    return __import__('hmac').new((secret or '').encode('utf-8'), raw.encode('utf-8'), __import__('hashlib').sha256).hexdigest()

def _issue_session(config_store) -> str:
    import secrets, time
    cfg = config_store.load()
    secret = str(cfg.get('session_secret') or '')
    raw = f"{int(time.time())}:{secrets.token_urlsafe(24)}"
    return raw + '.' + _session_sig(secret, raw)

def _admin_cookie_ok(handler: BaseHTTPRequestHandler, config_store) -> bool:
    if config_store is None:
        return False
    cfg = config_store.load()
    secret = str(cfg.get('session_secret') or '')
    sess = _cookie_dict(handler).get('embyrename_session') or ''
    if not secret or not sess or '.' not in sess:
        return False
    raw, sig = sess.rsplit('.', 1)
    return sig == _session_sig(secret, raw)


def make_handler(hub: LogHub, token: str) -> Type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "LogUI/3.0"

        def _send(self, code: int, body: bytes, ctype: str = "text/plain; charset=utf-8", extra_headers: dict | None = None):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            if extra_headers:
                for k, v in extra_headers.items():
                    self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _runtime(self):
            hooks = hub.get_runtime_hooks() if hasattr(hub, 'get_runtime_hooks') else {}
            getter = hooks.get('runtime_getter') if hooks else None
            if callable(getter):
                try:
                    return getter() or {}
                except Exception:
                    return {}
            return {'running': bool(getattr(hub, 'running', False)), 'stop_requested': hub.stop_requested()}

        def _config_store(self):
            hooks = hub.get_runtime_hooks() if hasattr(hub, 'get_runtime_hooks') else {}
            return hooks.get('config_store') if hooks else None

        def _is_admin(self) -> bool:
            return _admin_cookie_ok(self, self._config_store())

        def _auth_or_403(self) -> bool:
            if self._is_admin() or _token_ok(self, token):
                return True
            self._send(403, b"Forbidden\n")
            return False

        def _admin_or_403(self) -> bool:
            if self._is_admin():
                return True
            self._send(403, _json_bytes({'ok': False, 'error': 'admin required'}), 'application/json; charset=utf-8')
            return False

        def do_OPTIONS(self):
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Token")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.end_headers()

        def do_GET(self):
            if self.path.startswith('/api/auth/status'):
                store = self._config_store()
                has_password = bool(store.get_admin_password()) if store else False
                payload = {'ok': True, 'authenticated': self._is_admin(), 'initialized': has_password}
                self._send(200, _json_bytes(payload), 'application/json; charset=utf-8')
                return
            if self.path.startswith('/api/config'):
                if not self._admin_or_403():
                    return
                store = self._config_store()
                payload = {'ok': True, 'config': store.masked_config() if store else {}}
                self._send(200, _json_bytes(payload), 'application/json; charset=utf-8')
                return
            if self.path.startswith('/api/runtime'):
                if not self._auth_or_403():
                    return
                payload = {'ok': True, 'runtime': self._runtime()}
                self._send(200, _json_bytes(payload), 'application/json; charset=utf-8')
                return
            if self.path.startswith("/api/") or self.path.startswith("/events") or self.path.startswith("/export"):
                if not self._auth_or_403():
                    return
            if self.path.startswith("/api/stop"):
                try:
                    hub.request_stop()
                except Exception:
                    pass
                self._send(200, _json_bytes({'ok': True, 'stopping': True}), 'application/json; charset=utf-8')
                return
            if self.path.startswith("/api/stats"):
                st = hub.stats()
                counts_action = st.get("counts_action") or {}
                counts_level = st.get("counts_level") or {}
                payload = dict(st)
                payload.update({
                    "rename": int(counts_action.get("rename", 0) or 0),
                    "move": int(counts_action.get("move", 0) or 0),
                    "skip": int((counts_action.get("skip", 0) or 0) + (counts_action.get("dry", 0) or 0)),
                    "dry": int(counts_action.get("dry", 0) or 0),
                    "error": int(counts_level.get("ERROR", 0) or 0),
                })
                self._send(200, _json_bytes(payload), "application/json; charset=utf-8")
                return
            if self.path.startswith("/api/events"):
                parsed = urlparse(self.path)
                qs = parse_qs(parsed.query or "")
                try:
                    since = int((qs.get("since") or ["0"])[0] or "0")
                except Exception:
                    since = 0
                evs = [asdict(e) for e in hub.snapshot(limit=5000, since=since)]
                self._send(200, _json_bytes({'events': evs}), "application/json; charset=utf-8")
                return
            if self.path.startswith("/export.csv"):
                rows = ["ts,level,action,show,season,message,src,dst"]
                for e in hub.snapshot(limit=10000):
                    def esc(x: str) -> str:
                        x = (x or "").replace('"', '""')
                        return f'"{x}"'
                    rows.append(",".join([esc(e.ts), esc(e.level), esc(e.action), esc(e.show), esc(e.season), esc(e.message), esc(e.src), esc(e.dst)]))
                self._send(200, ("\n".join(rows)).encode("utf-8"), "text/csv; charset=utf-8")
                return
            if self.path.startswith("/events"):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.end_headers()
                q = hub.subscribe()
                try:
                    self.wfile.write(b":ok\n\n")
                    self.wfile.flush()
                    while True:
                        try:
                            ev = q.get(timeout=15)
                            data = json.dumps(asdict(ev), ensure_ascii=False).encode("utf-8")
                            self.wfile.write(b"data: " + data + b"\n\n")
                            self.wfile.flush()
                        except queue.Empty:
                            self.wfile.write(b":ping\n\n")
                            self.wfile.flush()
                except Exception:
                    pass
                finally:
                    hub.unsubscribe(q)
                return
            if self.path != '/' and not self._auth_or_403():
                return
            self._send(200, _INDEX_HTML.encode('utf-8'), 'text/html; charset=utf-8')

        def do_POST(self):
            if self.path.startswith('/api/auth/login') or self.path.startswith('/api/login'):
                store = self._config_store()
                if store is None:
                    self._send(500, _json_bytes({'ok': False, 'error': 'config store unavailable'}), 'application/json; charset=utf-8')
                    return
                payload = _read_json(self)
                password = str(payload.get('password') or '')
                cfg = store.load()
                encoded = str(cfg.get('admin_password_hash') or '')
                if not encoded:
                    if not password:
                        self._send(400, _json_bytes({'ok': False, 'error': 'password required'}), 'application/json; charset=utf-8')
                        return
                    cfg['admin_password_hash'] = hash_password(password)
                    store.save(cfg)
                    encoded = cfg['admin_password_hash']
                if not verify_password(password, encoded):
                    self._send(403, _json_bytes({'ok': False, 'error': 'invalid password'}), 'application/json; charset=utf-8')
                    return
                cookie = f"embyrename_session={_issue_session(store)}; HttpOnly; Path=/; SameSite=Lax"
                self._send(200, _json_bytes({'ok': True}), 'application/json; charset=utf-8', {'Set-Cookie': cookie})
                return
            if self.path.startswith('/api/auth/logout') or self.path.startswith('/api/logout'):
                self._send(200, _json_bytes({'ok': True}), 'application/json; charset=utf-8', {'Set-Cookie': 'embyrename_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax'})
                return
            if self.path.startswith('/api/config'):
                if not self._admin_or_403():
                    return
                store = self._config_store()
                payload = _read_json(self)
                cfg = store.save(payload or {}) if store else {}
                self._send(200, _json_bytes({'ok': True, 'config': store.masked_config() if store else cfg}), 'application/json; charset=utf-8')
                return
            if self.path.startswith('/api/tmdb/test'):
                if not self._admin_or_403():
                    return
                store = self._config_store()
                payload = _read_json(self) or {}
                cfg = dict(store.load()) if store else {}
                for k in ('tmdb_key', 'tmdb_lang'):
                    if k in payload and payload.get(k) not in (None, ''):
                        cfg[k] = payload.get(k)
                tmdb_key = str(cfg.get('tmdb_key') or '').strip()
                placeholder_keys = {'your_tmdb_key', 'tmdb_key', 'your-key', 'your_key', 'changeme'}
                if (not tmdb_key) or (tmdb_key.lower() in placeholder_keys):
                    self._send(400, _json_bytes({'ok': False, 'error': 'TMDB key 未配置，请填写真实可用的 API Key。'}), 'application/json; charset=utf-8')
                    return
                tmdb_lang = str(cfg.get('tmdb_lang') or 'zh-CN').strip() or 'zh-CN'
                try:
                    from renamer import TMDBClient
                    client = TMDBClient(api_key=tmdb_key, language=tmdb_lang, sleep=0)
                    sample = client.search_tv('Friends') or []
                    self._send(200, _json_bytes({'ok': True, 'message': 'TMDB Key 验证成功', 'sample_count': len(sample), 'base': getattr(client, 'base', '')}), 'application/json; charset=utf-8')
                except Exception as ex:
                    self._send(500, _json_bytes({'ok': False, 'error': str(ex) or ex.__class__.__name__}), 'application/json; charset=utf-8')
                return
            if self.path.startswith('/api/tmdb/cache/clear'):
                if not self._admin_or_403():
                    return
                try:
                    import renamer
                    cleared_mem = 0
                    cleared_file = False
                    cache_file = None
                    if hasattr(renamer, 'TMDB_CACHE') and isinstance(getattr(renamer, 'TMDB_CACHE'), dict):
                        cleared_mem = len(renamer.TMDB_CACHE)
                        renamer.TMDB_CACHE.clear()
                    cfg_dir_getter = getattr(renamer, 'get_config_dir', None)
                    if callable(cfg_dir_getter):
                        try:
                            cfg_dir = Path(cfg_dir_getter())
                            cache_file = cfg_dir / 'tmdb_cache.json'
                            if cache_file.exists():
                                cache_file.unlink()
                                cleared_file = True
                        except Exception:
                            logger.exception('[LOGUI] failed to remove tmdb cache file: %s', cache_file)
                    logger.info('[LOGUI] tmdb cache cleared mem=%s file=%s path=%s', cleared_mem, cleared_file, str(cache_file or ''))
                    message = f'已清除 TMDB 缓存：内存 {cleared_mem} 条'
                    if cache_file:
                        message += f"，文件缓存{'已删除' if cleared_file else '不存在'}"
                    self._send(200, _json_bytes({'ok': True, 'message': message, 'cleared': cleared_mem, 'file_deleted': cleared_file, 'cache_file': str(cache_file or '')}), 'application/json; charset=utf-8')
                except Exception as ex:
                    logger.exception('[LOGUI] clear tmdb cache failed')
                    self._send(500, _json_bytes({'ok': False, 'error': str(ex) or ex.__class__.__name__}), 'application/json; charset=utf-8')
                return
            if self.path.startswith('/api/alist/test'):
                if not self._admin_or_403():
                    return
                store = self._config_store()
                payload = _read_json(self) or {}
                cfg = dict(store.load()) if store else {}
                for k in ('alist_url', 'alist_token', 'alist_user', 'alist_pass', 'alist_otp'):
                    if k in payload and payload.get(k) not in (None, ''):
                        cfg[k] = payload.get(k)
                try:
                    AlistClient = _get_alist_client_cls()
                    username = str(cfg.get('alist_user') or '')
                    password = str(cfg.get('alist_pass') or '')
                    otp_code = str(cfg.get('alist_otp') or '')
                    preferred_token = str(cfg.get('alist_token') or '')
                    used_login = False
                    if username and password:
                        client = AlistClient(
                            base_url=str(cfg.get('alist_url') or ''),
                            token='',
                            username=username,
                            password=password,
                            otp_code=otp_code,
                        )
                        client.login_if_needed()
                        used_login = True
                    else:
                        client = AlistClient(
                            base_url=str(cfg.get('alist_url') or ''),
                            token=preferred_token,
                            username=username,
                            password=password,
                            otp_code=otp_code,
                        )
                        client.login_if_needed()
                    fresh_token = str(getattr(client, 'token', '') or '')
                    items = client.list_dirs_only('/') or []
                    if used_login and fresh_token:
                        cfg['alist_token'] = fresh_token
                        payload_save = dict(cfg)
                        payload_save['alist_token'] = fresh_token
                        if store:
                            store.save(payload_save)
                    self._send(200, _json_bytes({'ok': True, 'message': 'AList 连接成功', 'root_count': len(items), 'token': fresh_token, 'token_refreshed': bool(used_login and fresh_token)}), 'application/json; charset=utf-8')
                except Exception as ex:
                    self._send(500, _json_bytes({'ok': False, 'error': str(ex) or ex.__class__.__name__}), 'application/json; charset=utf-8')
                return
            if self.path.startswith('/api/alist/browse'):
                if not self._admin_or_403():
                    return
                store = self._config_store()
                payload = _read_json(self) or {}
                cfg = dict(store.load()) if store else {}
                for k in ('alist_url', 'alist_token', 'alist_user', 'alist_pass', 'alist_otp'):
                    if k in payload and payload.get(k) not in (None, ''):
                        cfg[k] = payload.get(k)
                raw_path = str(payload.get('path') or '/').strip()
                browse_path = _norm_path(raw_path or '/') or '/'
                auth_mode = 'password' if (cfg.get('alist_user') and cfg.get('alist_pass')) else ('token' if cfg.get('alist_token') else 'anonymous')
                started = time.monotonic()
                logger.info("[LOGUI] browse start path=%s base=%s auth=%s", browse_path, str(cfg.get('alist_url') or ''), auth_mode)
                try:
                    AlistClient = _get_alist_client_cls()
                    username = str(cfg.get('alist_user') or '')
                    password = str(cfg.get('alist_pass') or '')
                    otp_code = str(cfg.get('alist_otp') or '')
                    preferred_token = str(cfg.get('alist_token') or '')
                    used_login = False
                    if username and password:
                        client = AlistClient(
                            base_url=str(cfg.get('alist_url') or ''),
                            token='',
                            username=username,
                            password=password,
                            otp_code=otp_code,
                        )
                        client.login_if_needed()
                        used_login = True
                    else:
                        client = AlistClient(
                            base_url=str(cfg.get('alist_url') or ''),
                            token=preferred_token,
                            username=username,
                            password=password,
                            otp_code=otp_code,
                        )
                        client.login_if_needed()
                    fresh_token = str(getattr(client, 'token', '') or '')
                    items = client.list_dirs_only(browse_path) or []
                    if used_login and fresh_token:
                        cfg['alist_token'] = fresh_token
                        payload_save = dict(cfg)
                        payload_save['alist_token'] = fresh_token
                        if store:
                            store.save(payload_save)
                    parent = '/' if browse_path in ('', '/') else (_norm_path('/'.join(browse_path.rstrip('/').split('/')[:-1])) or '/')
                    dirs = []
                    for item in items:
                        if isinstance(item, dict):
                            name = str(item.get('name') or item.get('basename') or item.get('title') or '').strip()
                            full = _norm_path(item.get('path') or f"{browse_path.rstrip('/')}/{name}") if name else ''
                        else:
                            name = str(item).strip()
                            full = _norm_path(f"{browse_path.rstrip('/')}/{name}") if name else ''
                        if name:
                            dirs.append({'name': name, 'path': full or '/'})
                    dirs.sort(key=lambda x: x['name'])
                    logger.info("[LOGUI] browse success path=%s dirs=%s parent=%s token_refreshed=%s elapsed=%.3fs", browse_path, len(dirs), parent, bool(used_login and fresh_token), time.monotonic() - started)
                    self._send(200, _json_bytes({'ok': True, 'path': browse_path, 'parent': parent, 'dirs': dirs, 'token': fresh_token, 'token_refreshed': bool(used_login and fresh_token)}), 'application/json; charset=utf-8')
                except Exception as ex:
                    logger.exception("[LOGUI] browse failed path=%s base=%s auth=%s elapsed=%.3fs", browse_path, str(cfg.get('alist_url') or ''), auth_mode, time.monotonic() - started)
                    self._send(500, _json_bytes({'ok': False, 'error': str(ex) or ex.__class__.__name__}), 'application/json; charset=utf-8')
                return
            if self.path.startswith('/api/start'):
                if not self._admin_or_403():
                    return
                hooks = hub.get_runtime_hooks() if hasattr(hub, 'get_runtime_hooks') else {}
                fn = hooks.get('on_run') if hooks else None
                store = self._config_store()
                payload = _read_json(self) or {}
                if callable(fn):
                    try:
                        result = fn(payload or (store.load() if store else {}))
                    except Exception as ex:
                        msg = str(ex) or ex.__class__.__name__
                        lower_msg = msg.lower()
                        status = 409 if any(s in lower_msg for s in ('already running', 'in progress', 'busy', 'duplicate')) else 500
                        self._send(status, _json_bytes({'ok': False, 'error': msg}), 'application/json; charset=utf-8')
                        return
                    self._send(200, _json_bytes(result or {'ok': True}), 'application/json; charset=utf-8')
                else:
                    self._send(501, _json_bytes({'ok': False, 'error': 'start hook missing'}), 'application/json; charset=utf-8')
                return
            if self.path.startswith('/api/reload'):
                if not self._admin_or_403():
                    return
                hooks = hub.get_runtime_hooks() if hasattr(hub, 'get_runtime_hooks') else {}
                fn = hooks.get('on_reload') or hooks.get('on_run') if hooks else None
                store = self._config_store()
                payload = _read_json(self) or {}
                if callable(fn):
                    try:
                        result = fn(payload or (store.load() if store else {}))
                    except Exception as ex:
                        msg = str(ex) or ex.__class__.__name__
                        lower_msg = msg.lower()
                        status = 409 if any(s in lower_msg for s in ('already running', 'in progress', 'busy', 'duplicate')) else 500
                        self._send(status, _json_bytes({'ok': False, 'error': msg}), 'application/json; charset=utf-8')
                        return
                    self._send(200, _json_bytes(result or {'ok': True}), 'application/json; charset=utf-8')
                else:
                    self._send(501, _json_bytes({'ok': False, 'error': 'reload hook missing'}), 'application/json; charset=utf-8')
                return
            if self.path.startswith('/api/stop'):
                if not self._auth_or_403():
                    return
                hub.request_stop()
                self._send(200, _json_bytes({'ok': True, 'stopping': True}), 'application/json; charset=utf-8')
                return
            self._send(404, b'Not Found\n')

        def log_message(self, fmt, *args):
            return

    return Handler


_INDEX_HTML = r"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>EmbyRename 管理台</title>
  <style>
    :root{--bg:#eef2f7;--card:#fff;--muted:#64748b;--text:#0f172a;--line:#e2e8f0;--pri:#2563eb;--pri2:#7c3aed;--ok:#16a34a;--warn:#d97706;--err:#dc2626;--chip:#eef2ff;--soft:#f8fafc;--soft2:#eff6ff;--shadow:0 16px 40px rgba(15,23,42,.08)}
    *{box-sizing:border-box} html{scroll-behavior:smooth} body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,"PingFang SC","Microsoft YaHei",sans-serif;background:radial-gradient(900px 420px at 8% -5%, rgba(37,99,235,.18), transparent 62%),radial-gradient(720px 360px at 90% 0%, rgba(124,58,237,.13), transparent 58%),var(--bg);color:var(--text)}
    header{position:sticky;top:0;z-index:20;background:rgba(248,250,252,.86);backdrop-filter:blur(16px);border-bottom:1px solid rgba(226,232,240,.88)}
    .wrap{max-width:1480px;margin:0 auto;padding:14px 18px}
    .top{display:flex;justify-content:space-between;align-items:center;gap:14px;flex-wrap:wrap}
    h1{font-size:20px;margin:0;letter-spacing:.2px}.muted{color:var(--muted)} .hidden{display:none!important}
    .toolbar,.statusbar,.btns{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
    .pill{display:inline-flex;align-items:center;gap:8px;padding:6px 10px;border:1px solid rgba(226,232,240,.9);border-radius:999px;background:rgba(255,255,255,.88);font-size:12px;color:var(--muted);box-shadow:0 4px 14px rgba(15,23,42,.03)}
    .dot{width:8px;height:8px;border-radius:50%;background:#9ca3af;box-shadow:0 0 0 3px rgba(148,163,184,.14)}.dot.ok{background:var(--ok);box-shadow:0 0 0 3px rgba(22,163,74,.14)}.dot.err{background:var(--err);box-shadow:0 0 0 3px rgba(220,38,38,.14)}.dot.warn{background:var(--warn);box-shadow:0 0 0 3px rgba(217,119,6,.14)}
    main{max-width:1480px;margin:0 auto;padding:18px;display:grid;grid-template-columns:minmax(720px,1.18fr) minmax(420px,.82fr);align-items:start;align-content:start;gap:18px}@media (max-width:1200px){main{grid-template-columns:1fr}}
    .stack{display:grid;gap:18px;align-content:start}.side-stack{position:sticky;top:82px;max-height:calc(100vh - 96px);overflow:auto;padding-bottom:4px}@media (max-width:1200px){.side-stack{position:static;max-height:none;overflow:visible}}
    .card{background:rgba(255,255,255,.94);border:1px solid rgba(226,232,240,.92);border-radius:22px;overflow:hidden;box-shadow:var(--shadow)}
    .card h2{margin:0;padding:14px 16px;border-bottom:1px solid var(--line);font-size:14px;background:linear-gradient(180deg,#fff,#f8fafc)}.content{padding:16px}.hint{font-size:12px;color:var(--muted);line-height:1.55}
    .content.logpanel{display:flex;flex-direction:column;min-height:0;height:min(72vh,820px);padding:0}
    .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.grid3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}@media (max-width:760px){.grid,.grid3{grid-template-columns:1fr}}
    .field{display:grid;gap:7px}.field label{font-size:12px;color:#334155;font-weight:700}.field input,.field textarea,.field select,.filters input,.filters select{width:100%;border:1px solid var(--line);border-radius:12px;padding:10px 12px;font-size:13px;background:#fff;color:var(--text);outline:none;transition:border-color .15s,box-shadow .15s,background .15s}
    .field input:focus,.field textarea:focus,.field select:focus,.filters input:focus,.filters select:focus{border-color:#93c5fd;box-shadow:0 0 0 4px rgba(37,99,235,.09);background:#fff}
    .field textarea{min-height:96px;resize:vertical}.field .mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}.checkrow{display:flex;gap:10px;flex-wrap:wrap}.check{display:flex;gap:8px;align-items:center;font-size:13px;border:1px solid var(--line);background:#fff;border-radius:999px;padding:7px 10px}
    button{appearance:none;border:1px solid var(--line);background:#fff;color:var(--text);padding:8px 12px;border-radius:12px;font-size:13px;cursor:pointer;transition:transform .12s,border-color .12s,box-shadow .12s,background .12s}button:hover{border-color:#93c5fd;box-shadow:0 6px 16px rgba(37,99,235,.08);transform:translateY(-1px)}button.primary{background:linear-gradient(135deg,#2563eb,#4f46e5);border-color:transparent;color:#fff}button.danger{background:rgba(220,38,38,.06);border-color:rgba(220,38,38,.28);color:var(--err)}button:disabled{opacity:.55;cursor:not-allowed;transform:none;box-shadow:none}
    .stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}@media (max-width:760px){.stats{grid-template-columns:repeat(2,minmax(0,1fr))}} .stat{border:1px solid var(--line);border-radius:16px;padding:12px;background:linear-gradient(180deg,#fff,#f8fafc)}.stat .k{font-size:12px;color:var(--muted)}.stat .v{font-size:24px;margin-top:4px;font-weight:800;letter-spacing:-.03em}
    .runtime{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}@media (max-width:760px){.runtime{grid-template-columns:1fr}} .kv{border:1px solid var(--line);border-radius:14px;padding:10px;background:#fff}.kv .k{font-size:12px;color:var(--muted)}.kv .v{margin-top:4px;font-size:12px;line-height:1.35;word-break:break-word}
    .filters{display:grid;grid-template-columns:130px repeat(3,minmax(120px,1fr));gap:8px;padding:12px;border-bottom:1px solid var(--line);background:#f8fafc}.filters .filter-actions{grid-column:1 / -1;display:flex;gap:8px;flex-wrap:wrap}@media (max-width:760px){.filters{grid-template-columns:1fr}}
    .panel-tabs{display:flex;gap:8px;flex-wrap:wrap;padding:12px;border-bottom:1px solid var(--line);background:#f8fafc}
    .panel-tabs button.active{background:rgba(37,99,235,.12);border-color:rgba(37,99,235,.38);color:#1d4ed8;box-shadow:inset 0 0 0 1px rgba(37,99,235,.06)}
    .configview{flex:1 1 auto;min-height:0;overflow:auto;padding:12px;background:#fff;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}@media (max-width:1200px){.configview{height:58vh;flex:none}}@media (max-width:760px){.configview{grid-template-columns:1fr}}
    .configview pre{margin:0;white-space:pre-wrap;word-break:break-word;font:12px/1.55 ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;color:var(--text)}
    .configview .empty{color:var(--muted);font-size:13px;grid-column:1 / -1}
    .logbox{flex:1 1 auto;display:flex;flex-direction:column;min-height:0;padding:12px;overflow:hidden}@media (max-width:1200px){.logbox{height:58vh;flex:none}}
    .loggroups{flex:1 1 auto;min-height:0;overflow:auto;background:#fff;border:1px solid var(--line);border-radius:16px}
    .loghint{padding:10px 12px;border-bottom:1px solid var(--line);font-size:12px;color:var(--muted);background:#fafafa}
    .logline{display:grid;grid-template-columns:92px 56px minmax(0,1fr);gap:10px;align-items:start;padding:9px 12px;border-bottom:1px solid rgba(148,163,184,.10);font-size:12px}
    .ts{color:var(--muted);font-variant-numeric:tabular-nums}.lvl{font-weight:700}.main{min-width:0}.msg{white-space:pre-wrap;word-break:break-word;line-height:1.55}.meta{margin-top:4px;display:flex;flex-wrap:wrap;gap:6px 8px}.chip{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:999px;background:#f8fafc;border:1px solid #e5e7eb;color:#475569}.chip b{font-weight:700;color:#334155}.path{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;word-break:break-all}.lv-info .lvl{color:var(--ok)}.lv-warn .lvl{color:var(--warn)}.lv-error .lvl{color:var(--err)}.lv-skip .lvl{color:#7c3aed}.lv-dry .lvl{color:#db2777}
    .notice{padding:10px 12px;border-radius:12px;background:#f8fafc;border:1px dashed #cbd5e1;font-size:12px;color:var(--muted)} .danger-text{color:var(--err)}
    .hero{display:grid;gap:8px;padding:14px 16px;border:1px solid #dbeafe;border-radius:16px;background:linear-gradient(180deg, rgba(239,246,255,.95), rgba(248,250,252,.98))}
    .hero-title{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;flex-wrap:wrap}.hero-title strong{font-size:15px}.hero-list{margin:0;padding-left:18px;color:#475569;font-size:12px;line-height:1.6}
    .form-sections{display:grid;gap:12px;margin-top:12px}
    .section-card{border:1px solid var(--line);border-radius:16px;background:linear-gradient(180deg,#fff,#fcfdff)}
    .section-head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;flex-wrap:wrap;padding:12px 14px;border-bottom:1px solid var(--line);background:var(--soft)}
    .section-head strong{font-size:14px}.section-head .hint{max-width:720px}
    .section-body{padding:14px}
    .quicknav{display:flex;gap:8px;flex-wrap:wrap;margin-top:4px}.quicknav a{display:inline-flex;align-items:center;padding:7px 10px;border-radius:999px;background:#fff;border:1px solid #bfdbfe;color:#1d4ed8;text-decoration:none;font-size:12px;font-weight:700}.quicknav a:hover{background:#eff6ff}
    .section-card{scroll-margin-top:92px;overflow:hidden}.section-card[id]{border-color:#dbeafe}.section-card[id] .section-head{position:relative}.section-card[id] .section-head:before{content:'';position:absolute;left:0;top:0;bottom:0;width:4px;background:linear-gradient(180deg,var(--pri),var(--pri2))}
    .inline-actions{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
    .field-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;align-items:center}@media (max-width:760px){.field-row{grid-template-columns:1fr}}
    .chips{display:flex;gap:8px;flex-wrap:wrap}
    .tipbox{padding:10px 12px;border-radius:12px;background:var(--soft2);border:1px solid #bfdbfe;font-size:12px;color:#1e3a8a;line-height:1.6}
    .actions-bar{position:sticky;bottom:0;display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;padding:14px 16px;margin-top:14px;border:1px solid var(--line);border-radius:16px;background:rgba(255,255,255,.94);backdrop-filter:blur(8px);box-shadow:0 8px 24px rgba(15,23,42,.06)}
    .actions-bar .hint{margin:0}
    .toast-host{position:fixed;right:18px;bottom:18px;display:flex;flex-direction:column;gap:10px;z-index:1000;pointer-events:none;max-width:min(420px,calc(100vw - 32px))}
    .toast{pointer-events:auto;border-radius:12px;padding:12px 14px;border:1px solid rgba(148,163,184,.26);background:rgba(15,23,42,.94);color:#e5eefb;box-shadow:0 12px 30px rgba(0,0,0,.35);opacity:0;transform:translateY(8px);transition:opacity .18s ease,transform .18s ease}
    .toast.show{opacity:1;transform:translateY(0)}
    .toast.success{border-color:rgba(16,185,129,.38)} .toast.error{border-color:rgba(239,68,68,.45)} .toast.warn{border-color:rgba(245,158,11,.45)} .toast.info{border-color:rgba(96,165,250,.4)}
    .toast .title{font-weight:700;margin-bottom:4px} .toast .msg{font-size:13px;line-height:1.45;word-break:break-word}
    .toast .close{float:right;border:none;background:transparent;color:#94a3b8;cursor:pointer;font-size:16px;line-height:1;padding:0 0 0 8px}
    .btn-busy{position:relative;opacity:.78;cursor:progress} .btn-busy::after{content:'';width:12px;height:12px;border-radius:999px;border:2px solid rgba(255,255,255,.28);border-top-color:#fff;display:inline-block;margin-left:8px;vertical-align:-2px;animation:spin .8s linear infinite}
    .confirm-backdrop{position:fixed;inset:0;background:rgba(2,6,23,.62);display:flex;align-items:center;justify-content:center;padding:20px;z-index:1100}
    .confirm-dialog{width:min(460px,100%);background:#0f172a;border:1px solid rgba(148,163,184,.24);border-radius:16px;box-shadow:0 20px 50px rgba(0,0,0,.45);padding:18px;color:#e5eefb}
    .confirm-dialog h3{margin:0 0 8px;font-size:18px} .confirm-dialog p{margin:0;color:#cbd5e1;line-height:1.6;white-space:pre-wrap}
    .confirm-actions{display:flex;justify-content:flex-end;gap:10px;margin-top:18px}
    @keyframes spin{to{transform:rotate(360deg)}}
  </style>
</head>
<body>
<header>
  <div class="wrap top">
    <div>
      <h1>EmbyRename 管理台</h1>
      <div class="muted" id="topSubtitle">登录后可管理配置、启动/停止任务并查看日志</div>
    </div>
    <div class="toolbar">
      <span class="pill"><span class="dot" id="dotConn"></span><span id="connText">连接中…</span></span>
      <span class="pill"><span class="dot" id="dotAuth"></span><span id="authText">鉴权检查中…</span></span>
      <span class="pill"><span class="dot" id="dotRun"></span><span id="runText">任务状态：未知</span></span>
      <button id="btnLogout" class="hidden">登出</button>
      <button id="btnRefresh">刷新</button>
    </div>
  </div>
</header>
<main>
  <section class="stack">
    <div class="card" id="authCard">
      <h2 id="authCardTitle">管理员登录</h2>
      <div class="content">
        <div class="notice" id="authNotice">首次使用时，请设置管理员密码。</div>
        <div class="grid" style="margin-top:12px">
          <div class="field">
            <label for="loginPassword">管理员密码</label>
            <input id="loginPassword" type="password" autocomplete="current-password" placeholder="请输入密码" />
          </div>
        </div>
        <div class="btns" style="margin-top:12px">
          <button id="btnLogin" class="primary">登录 / 初始化密码</button>
        </div>
      </div>
    </div>

    <div class="card hidden" id="configCard">
      <h2>配置</h2>
      <div class="content">
        <div class="hero">
          <div class="hero-title">
            <strong>配置向导：连接鉴权 → 扫描来源 → 整理规则 → 保存启动</strong>
            <span class="pill">管理员可编辑</span>
          </div>
          <ul class="hero-list">
            <li>常用操作集中在底部浮动栏；启动前会提示保存未提交配置。</li>
            <li>“扫描根目录”优先；为空时才启用“自动发现扫描根目录”。</li>
            <li>右侧固定显示运行状态，可一键切换当前配置与实时日志。</li>
          </ul>
          <nav class="quicknav" aria-label="配置快速导航">
            <a href="#secConn">连接鉴权</a><a href="#secScan">扫描目录</a><a href="#secRules">整理规则</a><a href="#secCategory">分类结构</a><a href="#secRuntime">运行维护</a>
          </nav>
        </div>
        <form id="configForm">
          <div class="form-sections">
            <section class="section-card" id="secConn">
              <div class="section-head">
                <div>
                  <strong>连接与鉴权</strong>
                  <div class="hint">填写 AList、TMDB 与 AI 服务相关参数。通常先完成这里，再继续后续目录和规则配置。</div>
                </div>
              </div>
              <div class="section-body">
                <div class="grid">
                  <div class="field"><label>AList 地址</label><input name="alist_url" placeholder="https://alist.example.com" /></div>
                  <div class="field"><label>AList 访问令牌</label><input name="alist_token" type="text" placeholder="留空表示不变更" /><div class="hint">已配置用户名/密码时，也可先留空，连接后再自动刷新令牌。</div></div>
                  <div class="field"><label>AList 用户名</label><input name="alist_user" /></div>
                  <div class="field"><label>AList 密码</label><input name="alist_pass" type="text" placeholder="留空表示不变更" /></div>
                  <div class="field"><label>AList OTP 验证码</label><input name="alist_otp" type="text" placeholder="留空表示不变更" /><div class="hint">仅在 AList 开启二步验证时填写。</div></div>
                  <div class="field">
                    <label>TMDB 密钥</label>
                    <div class="field-row">
                      <input name="tmdb_key" type="text" placeholder="留空表示不变更" />
                      <button type="button" id="btnTestTmdb">验证 TMDB Key</button>
                      <button type="button" id="btnClearTmdbCache" class="danger">清 TMDB 缓存</button>
                    </div>
                  </div>
                  <div class="field"><label>TMDB 语言</label><input name="tmdb_lang" placeholder="zh-CN" /><div class="hint">常用值为 zh-CN；影响抓取到的标题与元数据语言。</div></div>
                  <div class="field"><label>AI 服务地址</label><input name="ai_base_url" placeholder="https://api.example.com/v1" /></div>
                  <div class="field"><label>AI 接口密钥</label><input name="ai_api_key" type="text" placeholder="留空表示不变更" /></div>
                  <div class="field"><label>AI 模型</label><input name="ai_model" /><div class="hint">仅在启用 AI 辅助识别时使用。</div></div>
                  <div class="field"><label>只读访问令牌</label><input name="readonly_token" type="text" placeholder="留空表示不变更" /><div class="hint">用于给访客提供查看日志/状态的只读入口。</div></div>
                  <div class="field"><label>对外访问地址</label><input name="public_host" /><div class="hint">用于展示或反代访问地址，不影响本地表单提交。</div></div>
                </div>
              </div>
            </section>

            <section class="section-card" id="secScan">
              <div class="section-head">
                <div>
                  <strong>扫描来源与目录选择</strong>
                  <div class="hint">决定从哪里扫描待整理内容。通常二选一：手动填写扫描根目录，或启用自动发现。</div>
                </div>
                <div class="chips">
                  <span class="pill">roots 优先</span>
                  <span class="pill">支持 AList 浏览</span>
                </div>
              </div>
              <div class="section-body">
                <div class="tipbox">若已手动填写“扫描根目录（roots）”，程序将直接按填写内容扫描，不再执行自动发现。只有当扫描根目录为空时，“自动发现扫描根目录”才会生效。</div>
                <div class="grid" style="margin-top:12px">
                  <div class="field">
                    <label>目标整理根目录（留空=自动回A/B顶层）</label>
                    <input name="target_root" class="mono" placeholder="留空=自动回来源顶层根；如 /整理=统一整理到固定根" />
                    <div class="inline-actions">
                      <button type="button" id="btnBrowseTarget">浏览 AList 目录</button>
                      <button type="button" id="btnTestAlist">测试 AList 连接</button>
                    </div>
                    <div class="hint">留空：自动回来源顶层根归档（A/任意层/视频→A/电影，B/任意层/视频→B/电影）。填写：忽略A/B来源，统一归档到你填的目录。</div>
                    <div id="alistBrowser" class="notice hidden" style="margin-top:8px">
                      <div style="display:flex;justify-content:space-between;gap:8px;align-items:center;flex-wrap:wrap">
                        <strong>AList 目录浏览</strong>
                        <span id="alistCurrentPath" class="mono">/</span>
                      </div>
                      <div class="btns" style="margin-top:8px">
                        <button type="button" id="btnAlistUp">上一级</button>
                        <button type="button" id="btnAlistUseCurrent" class="primary">使用当前目录</button>
                        <button type="button" id="btnAlistClose">关闭浏览器</button>
                      </div>
                      <div id="alistDirList" class="runtime" style="margin-top:10px"></div>
                    </div>
                  </div>
                  <div class="field"><label>扫描根目录（每行一个）</label><textarea name="roots" class="mono" placeholder="/media/Shows"></textarea><div class="hint">手动指定固定扫描入口；适合目录稳定、来源明确的场景。</div></div>
                  <div class="field"><label>排除文件夹（每行一个）</label><textarea name="exclude_roots" class="mono" placeholder="/天翼/影视一&#10;/天翼/回收站"></textarea><div class="hint">支持多个排除目录；扫描时会跳过这些目录及其子目录。若勾选“扫描时排除目标整理根目录”，会额外自动排除上方目标整理根目录。</div></div>
                  <div class="field"><label>自动发现根目录正则</label><input name="discover_root_regex" class="mono" placeholder="^(A|B)$" /><div class="hint">仅在启用自动发现且未填写扫描根目录时使用：先匹配顶层目录（如 A/B），再在其下做广度搜索；默认最多向下 3 层，凡命中“自动发现分类”的目录（如 电影/动漫/剧集）都会加入扫描根。</div></div>
                  <div class="field"><label>自动发现分类（逗号分隔）</label><textarea name="discover_categories" class="mono" placeholder="tv,anime"></textarea><div class="hint">限定自动发现时参与扫描的分类；多个值用英文逗号分隔。</div></div>
                </div>
                <div class="checkrow" style="margin-top:14px">
                  <label class="check"><input type="checkbox" name="auto_roots" /> 自动发现扫描根目录</label>
                  <label class="check"><input type="checkbox" name="scan_exclude_target" /> 扫描时排除目标整理根目录</label>
                  <label class="check"><input type="checkbox" name="resume" /> 断点续跑</label>
                </div>
              </div>
            </section>

            <section class="section-card" id="secRules">
              <div class="section-head">
                <div>
                  <strong>命名与整理规则</strong>
                  <div class="hint">决定如何重命名、如何生成季目录、如何组织目标目录树。</div>
                </div>
              </div>
              <div class="section-body">
                <div class="grid">
                  <div class="field"><label>关键字</label><input name="keyword" /><div class="hint">用于辅助识别目标内容，可留空。</div></div>
                  <div class="field"><label>季目录格式</label><input name="season_format" placeholder="Season {season:02d}" /><div class="hint">例如 Season 01、Season 02；按 Python format 风格填写。</div></div>
                  <div class="field"><label>最大剧集数</label><input name="max_series" type="number" /><div class="hint">超过该范围的数字不再按集号处理，可降低误判。</div></div>
                  <div class="field"><label>跳过目录正则</label><input name="skip_dir_regex" class="mono" /><div class="hint">匹配到的目录会直接跳过，不进入识别整理流程。</div></div>
                </div>
                <div class="checkrow" style="margin-top:14px">
                  <label class="check"><input type="checkbox" name="organize_enabled" /> 启用整理模式</label>
                  <label class="check"><input type="checkbox" name="init_target_tree" /> 保存时初始化整理目录树</label>
                  <label class="check"><input type="checkbox" name="rename_series" /> 重命名剧集目录</label>
                  <label class="check"><input type="checkbox" name="rename_files" /> 重命名文件</label>
                  <label class="check"><input type="checkbox" name="fix_bare_sxxeyy" /> 修复裸 SxxEyy</label>
                  <label class="check"><input type="checkbox" name="dry_run" /> 仅演练（dry-run）</label>
                  <label class="check"><input type="checkbox" name="no_ai" /> 禁用 AI</label>
                </div>
              </div>
            </section>

            <section class="section-card" id="secCategory">
              <div class="section-head">
                <div>
                  <strong>分类目录结构</strong>
                  <div class="hint">用于生成整理后的一级分类、地区细分与映射关系。适合整理模式启用时配置。</div>
                </div>
              </div>
              <div class="section-body">
                <div class="grid">
                  <div class="field"><label>一级分类（每行一个）</label><textarea name="category_buckets" class="mono" placeholder="电影&#10;剧集&#10;动漫&#10;纪录片&#10;综艺&#10;演唱会&#10;体育"></textarea></div>
                  <div class="field"><label>地区细分（每行一个）</label><textarea name="region_buckets" class="mono" placeholder="大陆&#10;港台&#10;欧美&#10;日韩&#10;其他"></textarea></div>
                  <div class="field" style="grid-column:1 / -1"><label>分类→地区映射（每行一项，格式：分类:地区1,地区2）</label><textarea name="category_region_map" class="mono" placeholder="电影:大陆,港台,欧美,日韩,其他&#10;剧集:大陆,港台,欧美,日韩,其他&#10;动漫:大陆,港台,欧美,日韩,其他&#10;纪录片&#10;综艺&#10;演唱会&#10;体育"></textarea><div class="hint">不带冒号时表示该分类可存在但不细分地区。</div></div>
                </div>
              </div>
            </section>

            <section class="section-card" id="secRuntime">
              <div class="section-head">
                <div>
                  <strong>运行与维护参数</strong>
                  <div class="hint">控制扫描节奏、日志保留、状态文件与网络容错行为。</div>
                </div>
              </div>
              <div class="section-body">
                <div class="grid">
                  <div class="field"><label>扫描间隔（秒）</label><input name="sleep" type="number" step="0.1" /></div>
                  <div class="field"><label>TMDB 请求间隔（秒）</label><input name="tmdb_sleep" type="number" step="0.1" /></div>
                  <div class="field"><label>AI 请求间隔（秒）</label><input name="ai_sleep" type="number" step="0.1" /></div>
                  <div class="field"><label>日志监听地址</label><input name="log_host" /></div>
                  <div class="field"><label>日志监听端口</label><input name="log_port" type="number" /></div>
                  <div class="field"><label>日志保留条数</label><input name="log_keep" type="number" /></div>
                  <div class="field"><label>状态文件路径</label><input name="state_file" class="mono" /></div>
                  <div class="field"><label>撤销日志路径</label><input name="undo_log" class="mono" /></div>
                </div>
                <div class="checkrow" style="margin-top:14px">
                  <label class="check"><input type="checkbox" name="insecure" /> 跳过 TLS 校验</label>
                </div>
              </div>
            </section>
          </div>

          <div class="actions-bar">
            <p class="hint">保存只会写入配置；“保存并重载”适合已运行任务；启动前建议先用 dry-run 验证。</p>
            <div class="btns">
              <button type="button" id="btnSave" class="primary">保存</button>
              <button type="button" id="btnSaveReload">保存并重载</button>
              <button type="button" id="btnStart">启动</button>
              <button type="button" id="btnStop" class="danger">停止</button>
            </div>
          </div>
        </form>
      </div>
    </div>
  </section>

  <section class="stack side-stack">
    <div class="card">
      <h2>运行状态</h2>
      <div class="content">
        <div class="stats">
          <div class="stat"><div class="k">已重命名</div><div class="v" id="stRen">0</div></div>
          <div class="stat"><div class="k">已移动</div><div class="v" id="stMov">0</div></div>
          <div class="stat"><div class="k">已跳过</div><div class="v" id="stSkip">0</div></div>
          <div class="stat"><div class="k">错误</div><div class="v" id="stErr">0</div></div>
        </div>
        <div class="runtime" style="margin-top:12px">
          <div class="kv"><div class="k">状态</div><div class="v" id="rtStatus">未知</div></div>
          <div class="kv"><div class="k">最近错误</div><div class="v" id="rtError">-</div></div>
          <div class="kv"><div class="k">最近启动</div><div class="v" id="rtStarted">-</div></div>
          <div class="kv"><div class="k">最近停止</div><div class="v" id="rtStopped">-</div></div>
        </div>
      </div>
    </div>

    <div class="card">
      <h2 id="rightPanelTitle">配置 / 日志</h2>
      <div class="content logpanel">
      <div class="panel-tabs">
        <button id="tabConfig" type="button" class="active">当前保存配置</button>
        <button id="tabLogs" type="button">实时日志</button>
      </div>
      <div class="filters hidden" id="logFilters">
        <select id="fLevel"><option value="ALL">全部级别</option><option value="ERROR">ERROR</option><option value="WARN">WARN</option><option value="INFO">INFO</option><option value="SKIP">SKIP</option><option value="DRY">DRY</option></select>
        <input id="fSeason" placeholder="季过滤：如 S01" />
        <input id="fShow" placeholder="剧名过滤：如 暗河传" />
        <input id="fKeyword" placeholder="关键词过滤：如 重命名 / tmdb" />
        <div class="filter-actions">
          <button id="btnReset">重置过滤</button>
          <button id="btnExpandAll">滚到最新</button>
          <button id="btnCollapseAll">滚到最早</button>
          <button id="btnClearLocal">清空本页缓存</button>
        </div>
      </div>
      <div class="configview" id="configView"><div class="empty">正在读取当前保存的配置…</div></div>
      <div class="logbox loggroups hidden" id="logGroups"></div>
      </div>
    </div>
  </section>
</main>
<div id="toastHost" class="toast-host" aria-live="polite" aria-atomic="true"></div>
<script>
(function(){
  const $ = (id)=>document.getElementById(id);
  const authCard = $('authCard'), configCard = $('configCard'), authCardTitle = $('authCardTitle'), authNotice = $('authNotice');
  const loginPassword = $('loginPassword'), btnLogin = $('btnLogin'), btnLogout = $('btnLogout'), btnRefresh = $('btnRefresh');
  const btnSave = $('btnSave'), btnSaveReload = $('btnSaveReload'), btnStart = $('btnStart'), btnStop = $('btnStop');
  const btnBrowseTarget = $('btnBrowseTarget'), btnTestAlist = $('btnTestAlist'), btnAlistUp = $('btnAlistUp'), btnAlistUseCurrent = $('btnAlistUseCurrent'), btnAlistClose = $('btnAlistClose');
  const btnTestTmdb = $('btnTestTmdb');
  const alistBrowser = $('alistBrowser'), alistCurrentPath = $('alistCurrentPath'), alistDirList = $('alistDirList');
  const dotConn = $('dotConn'), connText = $('connText'), dotAuth = $('dotAuth'), authText = $('authText'), dotRun = $('dotRun'), runText = $('runText');
  const rtStatus = $('rtStatus'), rtError = $('rtError'), rtStarted = $('rtStarted'), rtStopped = $('rtStopped');
  const stRen = $('stRen'), stMov = $('stMov'), stSkip = $('stSkip'), stErr = $('stErr');
  const groupsEl = $('logGroups'), fLevel = $('fLevel'), fSeason = $('fSeason'), fShow = $('fShow'), fKeyword = $('fKeyword');
  const btnExpandAll = $('btnExpandAll'), btnCollapseAll = $('btnCollapseAll'), btnReset = $('btnReset'), btnClearLocal = $('btnClearLocal');
  const form = $('configForm');
  const toastHost = $('toastHost');

  let authState = {initialized:false, authenticated:false, readonly:false};
  let runtimeState = {};
  let events = [];
  let lastId = 0;
  let alistPath = '/';
  let formDirty = false;
  let suppressDirty = false;
  const groups = new Map();

  const textFields = ['alist_url','alist_user','tmdb_lang','ai_base_url','ai_model','keyword','season_format','state_file','undo_log','skip_dir_regex','discover_root_regex','log_host','public_host','target_root'];
  const numberFields = ['max_series','sleep','tmdb_sleep','ai_sleep','log_port','log_keep'];
  const boolFields = ['auto_roots','rename_series','rename_files','fix_bare_sxxeyy','dry_run','resume','insecure','no_ai','organize_enabled','scan_exclude_target','init_target_tree'];
  const sensitiveFields = ['alist_token','alist_pass','alist_otp','tmdb_key','ai_api_key','readonly_token'];
  const listFields = ['roots','discover_categories','category_buckets','region_buckets'];
  const csvFields = ['discover_categories'];

  function setConn(ok){ dotConn.className = ok ? 'dot ok' : 'dot err'; connText.textContent = ok ? '已连接' : '连接失败'; }
  function setAuth(){
    const readonly = !!authState.readonly && !authState.authenticated;
    dotAuth.className = authState.authenticated ? 'dot ok' : (readonly ? 'dot ok' : (authState.initialized ? 'dot warn' : 'dot'));
    authText.textContent = authState.authenticated ? '已登录' : (readonly ? '只读访客' : (authState.initialized ? '未登录' : '未初始化'));
    authCard.classList.toggle('hidden', authState.authenticated || readonly);
    configCard.classList.toggle('hidden', !authState.authenticated);
    btnLogout.classList.toggle('hidden', !authState.authenticated);
    authCardTitle.textContent = authState.initialized ? '管理员登录' : '初始化管理员密码';
    authNotice.textContent = readonly
      ? '当前为只读访客模式，可查看日志与运行状态；配置修改需管理员登录。'
      : (authState.initialized ? '请输入管理员密码登录。' : '首次使用时，请设置管理员密码。');
    btnLogin.textContent = readonly ? '管理员登录' : (authState.initialized ? '登录' : '初始化密码');
    loginPassword.placeholder = authState.initialized ? '请输入密码' : '请设置管理员密码';
  }
  function setRun(rt){
    runtimeState = rt || {};
    const running = !!rt.running || String(rt.state||'').toLowerCase() === 'running';
    dotRun.className = running ? 'dot ok' : ((String(rt.state||'').toLowerCase()==='error') ? 'dot err' : 'dot');
    runText.textContent = '任务状态：' + (rt.state || (running ? 'running' : 'idle') || 'unknown');
    rtStatus.textContent = rt.state || (running ? 'running' : 'idle');
    rtError.textContent = rt.last_error || '-';
    rtStarted.textContent = rt.last_start_time || rt.started_at || '-';
    rtStopped.textContent = rt.last_stop_time || rt.stopped_at || '-';
  }
  function escapeHtml(s){ return String(s||'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#039;'); }
  function getShow(ev){ return ev.show || '(未命名)'; }
  function levelKey(lv){ const u = String(lv||'INFO').toUpperCase(); return u === 'WARNING' ? 'WARN' : u; }
  function levelClass(lv){ const u = levelKey(lv); if(u==='ERROR') return 'lv-error'; if(u==='WARN') return 'lv-warn'; if(u==='SKIP') return 'lv-skip'; if(u==='DRY') return 'lv-dry'; return 'lv-info'; }
  function fmtSavedValue(val){
    if(Array.isArray(val)) return val.length ? val.join('\n') : '(空)';
    if(val && typeof val === 'object'){
      const lines = [];
      for(const [k, v] of Object.entries(val)) lines.push(`${k}: ${Array.isArray(v) ? v.join(', ') : (v == null ? '' : String(v))}`);
      return lines.length ? lines.join('\n') : '(空对象)';
    }
    if(val === '' || val == null) return '(空)';
    return String(val);
  }
  function renderSavedConfig(cfg){
    if(!configView) return;
    const fields = [
      ['AList 地址', cfg.alist_url], ['AList 访问令牌', cfg.alist_token], ['AList 用户名', cfg.alist_user], ['AList 密码', cfg.alist_pass],
      ['AList OTP 验证码', cfg.alist_otp], ['TMDB 密钥', cfg.tmdb_key], ['TMDB 语言', cfg.tmdb_lang], ['AI 服务地址', cfg.ai_base_url],
      ['AI 接口密钥', cfg.ai_api_key], ['AI 模型', cfg.ai_model], ['关键字', cfg.keyword], ['季目录格式', cfg.season_format],
      ['目标整理根目录', cfg.target_root], ['启用整理模式', cfg.organize_enabled], ['最大剧集数', cfg.max_series], ['扫描间隔（秒）', cfg.sleep], ['TMDB 请求间隔（秒）', cfg.tmdb_sleep],
      ['AI 请求间隔（秒）', cfg.ai_sleep], ['日志监听地址', cfg.log_host], ['日志监听端口', cfg.log_port], ['日志保留条数', cfg.log_keep],
      ['状态文件路径', cfg.state_file], ['撤销日志路径', cfg.undo_log], ['跳过目录正则', cfg.skip_dir_regex], ['自动发现根目录正则', cfg.discover_root_regex],
      ['扫描根目录', cfg.roots], ['排除文件夹', cfg.exclude_roots], ['自动发现分类', cfg.discover_categories], ['一级分类', cfg.category_buckets], ['地区细分', cfg.region_buckets],
      ['分类→地区映射', cfg.category_region_map], ['对外访问地址', cfg.public_host], ['只读访问令牌', cfg.readonly_token],
    ];
    configView.innerHTML = fields.map(([label, value])=>`<div class="kv"><div class="k">${escapeHtml(label)}</div><div class="v"><pre>${escapeHtml(fmtSavedValue(value))}</pre></div></div>`).join('') || '<div class="empty">暂无已保存配置</div>';
  }
  function showRightPanel(mode){
    const isLogs = mode === 'logs';
    if(tabConfig) tabConfig.classList.toggle('active', !isLogs);
    if(tabLogs) tabLogs.classList.toggle('active', isLogs);
    if(logFilters) logFilters.classList.toggle('hidden', !isLogs);
    if(configView) configView.classList.toggle('hidden', isLogs);
    if(groupsEl) groupsEl.classList.toggle('hidden', !isLogs);
    if(rightPanelTitle) rightPanelTitle.textContent = isLogs ? '实时日志' : '当前保存配置';
  }

  function formValue(name){ const el = form.elements.namedItem(name); return el ? el.value : ''; }
  function setDirty(dirty){
    formDirty = !!dirty;
    const changed = formDirty ? '有未保存更改' : '配置已同步';
    if(btnSave) btnSave.disabled = !authState.authenticated || !formDirty;
    if(btnSaveReload) btnSaveReload.disabled = !authState.authenticated || !formDirty;
    if(configCard){
      configCard.dataset.dirty = formDirty ? '1' : '0';
      configCard.classList.toggle('dirty', formDirty);
    }
    document.title = (formDirty ? '* ' : '') + 'Emby Rename 控制台';
    if(authState.authenticated && authNotice && authNotice.textContent !== changed) authNotice.textContent = changed;
  }
  function markDirty(){ if(!suppressDirty) setDirty(true); }
  function setValue(name, value){ const el = form.elements.namedItem(name); if(!el) return; if(el.tagName === 'TEXTAREA'){ el.value = Array.isArray(value) ? value.join('\n') : (value == null ? '' : String(value)); } else { el.value = value == null ? '' : String(value); } }
  function setChecked(name, value){ const el = form.elements.namedItem(name); if(el) el.checked = !!value; }
  function isMaskedValue(val){ return !!val && /^(\*+|.{0,4}\*+.{0,4})$/.test(String(val).trim()); }
  function payloadFromForm(includeSensitive){
    const payload = {};
    for(const k of textFields) payload[k] = formValue(k).trim();
    for(const k of numberFields){ const raw = formValue(k).trim(); payload[k] = raw === '' ? '' : Number(raw); }
    for(const k of boolFields){ const el = form.elements.namedItem(k); payload[k] = !!(el && el.checked); }
    payload.roots = formValue('roots').split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
    payload.exclude_roots = formValue('exclude_roots').split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
    payload.discover_categories = formValue('discover_categories').split(',').map(s=>s.trim()).filter(Boolean);
    payload.category_buckets = formValue('category_buckets').split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
    payload.region_buckets = formValue('region_buckets').split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
    payload.category_region_map = {};
    for(const line of formValue('category_region_map').split(/\r?\n/)){
      const raw = line.trim();
      if(!raw) continue;
      const idx = raw.indexOf(':');
      if(idx < 0){ payload.category_region_map[raw] = []; continue; }
      const cat = raw.slice(0, idx).trim();
      const regions = raw.slice(idx + 1).split(',').map(s=>s.trim()).filter(Boolean);
      if(cat) payload.category_region_map[cat] = regions;
    }
    if(includeSensitive){
      for(const k of sensitiveFields){
        const val = formValue(k);
        if(!isMaskedValue(val)) payload[k] = val;
      }
    }
    else{
      for(const k of sensitiveFields){ const val = formValue(k); if(val !== '' && !isMaskedValue(val)) payload[k] = val; }
    }
    return payload;
  }
  function fillForm(cfg){
    cfg = cfg || {};
    suppressDirty = true;
    try{
      for(const k of textFields) setValue(k, cfg[k]);
      for(const k of numberFields) setValue(k, cfg[k]);
      for(const k of boolFields) setChecked(k, cfg[k]);
      setValue('roots', cfg.roots || []);
      setValue('exclude_roots', cfg.exclude_roots || []);
      setValue('discover_categories', Array.isArray(cfg.discover_categories) ? cfg.discover_categories.join(', ') : (cfg.discover_categories || ''));
      setValue('category_buckets', cfg.category_buckets || []);
      setValue('region_buckets', cfg.region_buckets || []);
      if(cfg.category_region_map && typeof cfg.category_region_map === 'object' && !Array.isArray(cfg.category_region_map)){
        const lines = Object.entries(cfg.category_region_map).map(([cat, regions]) => `${cat}:${Array.isArray(regions) ? regions.join(',') : ''}`);
        setValue('category_region_map', lines);
      }else{
        setValue('category_region_map', cfg.category_region_map || '');
      }
      for(const k of sensitiveFields) setValue(k, cfg[k] || '');
    }finally{
      suppressDirty = false;
    }
    setDirty(false);
  }

  async function api(path, options){
    const res = await fetch(path, Object.assign({credentials:'same-origin'}, options || {}));
    let js = {};
    try{ js = await res.json(); }catch(e){}
    if(!res.ok || js.ok === false){
      const err = new Error(js.error || ('HTTP ' + res.status));
      err.status = res.status;
      err.payload = js;
      throw err;
    }
    return js;
  }
  function toast(msg, kind){
    const text = String(msg || '');
    authNotice.textContent = text;
    if(!toastHost) return;
    const level = ['success','error','warn','info'].includes(kind) ? kind : 'info';
    const el = document.createElement('div');
    el.className = 'toast ' + level;
    el.innerHTML = `<button type="button" class="close" aria-label="关闭">×</button><div class="title">${escapeHtml(level === 'error' ? '操作失败' : (level === 'success' ? '操作成功' : (level === 'warn' ? '操作提示' : '操作通知')))}</div><div class="msg">${escapeHtml(text)}</div>`;
    const remove = ()=>{
      el.classList.remove('show');
      setTimeout(()=>{ if(el.parentNode) el.parentNode.removeChild(el); }, 180);
    };
    el.querySelector('.close').addEventListener('click', remove);
    toastHost.appendChild(el);
    requestAnimationFrame(()=>el.classList.add('show'));
    setTimeout(remove, level === 'error' ? 5200 : 3200);
  }
  function setBusy(btn, busy, pendingText){
    if(!btn) return;
    if(busy){
      if(!btn.dataset.label) btn.dataset.label = btn.textContent;
      btn.disabled = true;
      btn.classList.add('btn-busy');
      if(pendingText) btn.textContent = pendingText;
    }else{
      btn.disabled = false;
      btn.classList.remove('btn-busy');
      if(btn.dataset.label) btn.textContent = btn.dataset.label;
    }
  }
  async function withBusy(btn, pendingText, action){
    if(btn && btn.disabled) return;
    setBusy(btn, true, pendingText);
    try{ return await action(); }
    finally{ setBusy(btn, false); }
  }
  function confirmAction(message, title){
    return new Promise((resolve)=>{
      const backdrop = document.createElement('div');
      backdrop.className = 'confirm-backdrop';
      backdrop.innerHTML = `<div class="confirm-dialog" role="dialog" aria-modal="true"><h3>${escapeHtml(title || '请确认操作')}</h3><p>${escapeHtml(message || '')}</p><div class="confirm-actions"><button type="button" data-act="cancel">取消</button><button type="button" class="danger" data-act="ok">确认</button></div></div>`;
      const close = (val)=>{ if(backdrop.parentNode) backdrop.parentNode.removeChild(backdrop); resolve(val); };
      backdrop.addEventListener('click', (ev)=>{ if(ev.target === backdrop) close(false); });
      backdrop.querySelector('[data-act="cancel"]').addEventListener('click', ()=>close(false));
      backdrop.querySelector('[data-act="ok"]').addEventListener('click', ()=>close(true));
      document.body.appendChild(backdrop);
    });
  }
  function alistPayload(){
    const payload = {};
    for(const k of ['alist_url','alist_token','alist_user','alist_pass','alist_otp']){
      const v = formValue(k);
      if(v === '') continue;
      if(sensitiveFields.includes(k) && isMaskedValue(v)) continue;
      payload[k] = v;
    }
    return payload;
  }
  function tmdbPayload(){
    const payload = {};
    for(const k of ['tmdb_key','tmdb_lang']){
      const v = formValue(k);
      if(v === '') continue;
      if(sensitiveFields.includes(k) && isMaskedValue(v)) continue;
      payload[k] = v;
    }
    return payload;
  }
  function renderAlistDirs(items){
    alistDirList.innerHTML = '';
    if(!Array.isArray(items) || !items.length){
      alistDirList.innerHTML = '<div class="kv"><div class="v">当前目录下没有子目录</div></div>';
      return;
    }
    for(const item of items){
      const wrap = document.createElement('div');
      wrap.className = 'kv';
      const title = document.createElement('div');
      title.className = 'k';
      title.textContent = item.path || '/';
      const row = document.createElement('div');
      row.className = 'btns';
      row.style.marginTop = '6px';
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.textContent = item.name || item.path || '(未命名目录)';
      btn.addEventListener('click', ()=>withBusy(btn, '读取中...', async ()=>{
        try{ await browseAlist(item.path || '/'); }
        catch(e){ toast('读取目录失败：' + e.message, 'error'); throw e; }
      }));
      const useBtn = document.createElement('button');
      useBtn.type = 'button';
      useBtn.className = 'primary';
      useBtn.textContent = '选择此目录';
      useBtn.addEventListener('click', ()=>{ setValue('target_root', item.path || '/'); markDirty(); toast('已选择目录：' + (item.path || '/'), 'success'); });
      row.append(btn, useBtn);
      wrap.append(title, row);
      alistDirList.appendChild(wrap);
    }
  }
  async function testAlist(){
    const js = await api('/api/alist/test', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(alistPayload())});
    if(js && js.token){
      setValue('alist_token', js.token);
    }
    toast((js.message || 'AList 连接成功') + (js.token_refreshed ? '，已自动刷新 Token' : '') + (js.root_count != null ? ('，根目录子目录数：' + js.root_count) : ''));
  }
  async function testTmdb(){
    const js = await api('/api/tmdb/test', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(tmdbPayload())});
    toast((js.message || 'TMDB Key 验证成功') + (js.base ? ('，接口：' + js.base) : '') + (js.sample_count != null ? ('，样本结果数：' + js.sample_count) : ''));
    return js;
  }
  async function browseAlist(path){
    const browsePath = (path == null || path === '') ? '/' : path;
    const js = await api('/api/alist/browse', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(Object.assign(alistPayload(), {path: browsePath}))});
    if(js && js.token){
      setValue('alist_token', js.token);
    }
    alistPath = js.path || '/';
    alistCurrentPath.textContent = alistPath;
    alistBrowser.classList.remove('hidden');
    btnAlistUp.disabled = !js.parent || js.parent === alistPath;
    btnAlistUp.dataset.parent = js.parent || '/';
    renderAlistDirs(js.dirs || []);
    return js;
  }

  function fmtTs(ev){
    const raw = ev.ts ?? ev.time ?? ev.timestamp ?? ev.created_at ?? ev.createdAt ?? null;
    if(raw == null || raw === '') return '';
    let d = null;
    if(typeof raw === 'number'){
      d = new Date(raw > 1e12 ? raw : raw * 1000);
    }else{
      const s = String(raw).trim();
      if(/^\d+(\.\d+)?$/.test(s)){
        const n = Number(s);
        d = new Date(n > 1e12 ? n : n * 1000);
      }else{
        d = new Date(s);
      }
    }
    if(!d || Number.isNaN(d.getTime())) return String(raw);
    const pad = (n)=>String(n).padStart(2, '0');
    return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }
  function shouldStickToBottom(){ return !!groupsEl && (groupsEl.scrollHeight - groupsEl.scrollTop - groupsEl.clientHeight) < 40; }
  function scrollLogsToBottom(){ if(groupsEl) groupsEl.scrollTop = groupsEl.scrollHeight; }
  function passes(ev){ const lv=levelKey(ev.level); if(fLevel.value!=='ALL' && lv!==fLevel.value) return false; if(fSeason.value && !String(ev.season||'').includes(fSeason.value)) return false; const show=getShow(ev); if(fShow.value && !show.includes(fShow.value)) return false; if(fKeyword.value){ const hay = show+' '+(ev.message||ev.msg||'')+' '+(ev.path||'')+' '+(ev.src||'')+' '+(ev.dst||''); if(!hay.includes(fKeyword.value)) return false; } return true; }
  function fmtPath(v){ return escapeHtml(String(v || '')); }
  function chip(label, value, cls=''){ return value ? `<span class="chip ${cls}"><b>${escapeHtml(label)}</b>${escapeHtml(String(value))}</span>` : ''; }
  function pathChip(label, value){ return value ? `<span class="chip path"><b>${escapeHtml(label)}</b>${fmtPath(value)}</span>` : ''; }
  function cleanSubject(raw){
    let s = String(raw || '').trim();
    s = s.replace(/^\[(DRY|INFO|WARN|ERROR|SKIP)\]\s*/i, '').trim();
    if(/^\[.*\]$/.test(s)) s = s.slice(1, -1).trim();
    s = s.replace(/^['"]+|['"]+$/g, '').trim();
    return s;
  }
  function summarizeMove(msg, ev){
    const raw = String(msg || '');
    const m = raw.match(/^(?:\[(DRY|INFO|WARN|ERROR|SKIP)\]\s*)?(move|rename)\s+(.*?)\s*:\s*(.*?)\s*->\s*(.*)$/i)
      || raw.match(/^(?:\[(DRY|INFO|WARN|ERROR|SKIP)\]\s*)?(rename)\s+(.*?)\s*->\s*(.*)$/i);
    if(!m) return null;
    if(m.length === 6){
      const action = m[2].toUpperCase();
      const from = m[4] || ev.src || ev.path || '';
      const subject = cleanSubject(action === 'RENAME' && from ? String(from).split('/').filter(Boolean).pop() : m[3]);
      return {levelHint:(m[1]||'').toUpperCase(), action, subject, from, to:m[5] || ev.dst || ''};
    }
    const from = ev.src || ev.path || '';
    const subject = cleanSubject(from ? String(from).split('/').filter(Boolean).pop() : m[2]);
    return {levelHint:(m[1]||'').toUpperCase(), action:(m[2]||'RENAME').toUpperCase(), subject, from, to:m[3] || ev.dst || ''};
  }
  function renderMeta(ev, show){
    const bits = [];
    const parsed = summarizeMove(ev.message || ev.msg || '', ev);
    if(show && (!parsed || show !== parsed.subject)) bits.push(chip('条目', show));
    if(ev.season) bits.push(chip('季', ev.season));
    if(parsed){
      if(parsed.subject) bits.push(chip('对象', parsed.subject));
      if(parsed.from) bits.push(pathChip('从', parsed.from));
      if(parsed.to) bits.push(pathChip('到', parsed.to));
    }else{
      if(ev.path) bits.push(pathChip('路径', ev.path));
      if(ev.src) bits.push(pathChip('源', ev.src));
      if(ev.dst) bits.push(pathChip('目标', ev.dst));
    }
    return bits.length ? `<div class="meta">${bits.join('')}</div>` : '';
  }
  function humanMsg(ev){
    const raw = String(ev.message || ev.msg || '');
    const lv = levelKey(ev.level);
    if(lv === 'ERROR') return raw;
    const parsed = summarizeMove(raw, ev);
    if(parsed){
      const act = parsed.action === 'RENAME' ? '重命名' : '移动';
      return parsed.subject ? `${act}：${parsed.subject}` : act;
    }
    let m = raw.match(/^\s*===\s*PROCESS:\s*(.*?)\s*=*\s*$/i);
    if(m) return `开始处理：${cleanSubject(m[1])}`;
    m = raw.match(/^\[SKIP\]\s*TMDB not found for:\s*(.*?)\s*$/i);
    if(m) return `TMDB 未找到：${cleanSubject(m[1])}`;
    m = raw.match(/^\[INFO\]\s*detected collection container:\s*(.*?)\s*$/i);
    if(m) return `检测到合集目录：${cleanSubject(m[1])}`;
    m = raw.match(/^\[RESUME\]\s*loaded\s+(\d+)\s+done series from:\s*(.*?)\s*$/i);
    if(m) return `断点续跑：已加载 ${m[1]} 条完成记录`;
    m = raw.match(/^\[WEBUI\]\s*config saved; task starting\s*$/i);
    if(m) return '配置已保存，任务启动中';
    m = raw.match(/^\[WEBUI\]\s*config center ready\s*$/i);
    if(m) return '控制台已就绪';
    m = raw.match(/^\[WEBUI\]\s*open:\s*(.*?)\s*$/i);
    if(m) return '控制台访问地址已生成';
    m = raw.match(/^\[DONE\]\s*Log saved:\s*(.*?)\s*$/i);
    if(m) return '运行日志已保存';
    return raw;
  }
  function addEventToDom(ev){
    const stick = shouldStickToBottom();
    const lv = levelKey(ev.level);
    const ts = fmtTs(ev);
    const msg = humanMsg(ev);
    const show = getShow(ev);
    const li = document.createElement('div');
    li.className = 'logline ' + levelClass(lv);
    li.innerHTML = `<span class="ts">${escapeHtml(ts)}</span><span class="lvl">${escapeHtml(lv)}</span><div class="main"><div class="msg">${escapeHtml(msg)}</div>${renderMeta(ev, show)}</div>`;
    groupsEl.appendChild(li);
    while(groupsEl.children.length > 1600) groupsEl.removeChild(groupsEl.firstChild);
    if(stick) requestAnimationFrame(scrollLogsToBottom);
  }
  function rebuild(){ groupsEl.innerHTML=''; for(const ev of events) if(passes(ev)) addEventToDom(ev); requestAnimationFrame(scrollLogsToBottom); }

  async function refreshAuth(){
    try{ authState = await api('/api/auth/status'); setConn(true); setAuth(); return authState; }
    catch(e){ setConn(!(e && e.status >= 500)); throw e; }
  }
  async function refreshConfig(){ if(!authState.authenticated) return; const js = await api('/api/config'); fillForm(js.config || {}); renderSavedConfig(js.config || {}); showRightPanel('config'); }
  async function refreshRuntime(){
    try{
      const js = await api('/api/runtime'); setRun(js.runtime || {}); setConn(true);
    }catch(e){
      if(e && (e.status === 401 || e.status === 403)){
        setConn(true);
        setRun(Object.assign({}, runtimeState, {state: authState.authenticated ? (runtimeState.state || 'idle') : '需登录后查看'}));
        return;
      }
      setConn(false);
      setRun(Object.assign({}, runtimeState, {state: '连接失败'}));
    }
  }
  async function refreshStats(){
    try{ const js = await api('/api/stats'); stRen.textContent = js.rename || 0; stMov.textContent = js.move || 0; stSkip.textContent = js.skip || 0; stErr.textContent = js.error || 0; setConn(true); }
    catch(e){
      setConn(!(e && (e.status >= 500)));
      if(e && (e.status === 401 || e.status === 403)){
        stRen.textContent = '—'; stMov.textContent = '—'; stSkip.textContent = '—'; stErr.textContent = '—';
      }
    }
  }
  async function poll(){
    try{ const js = await api('/api/events?since=' + encodeURIComponent(String(lastId||0))); if(Array.isArray(js.events)){ for(const ev of js.events){ events.push(ev); lastId = Math.max(lastId, ev.id || 0); if(passes(ev)) addEventToDom(ev); } } if(events.length > 6000) events = events.slice(events.length - 6000); setConn(true); }
    catch(e){ setConn(!(e && (e.status === 401 || e.status === 403 || e.status >= 500))); }
  }

  async function saveConfig(){ const js = await api('/api/config', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payloadFromForm(false))}); fillForm(js.config || {}); renderSavedConfig(js.config || {}); }
  async function saveAndReload(){ const payload = payloadFromForm(false); const saved = await api('/api/config', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)}); renderSavedConfig(saved.config || {}); await api('/api/reload', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({})}); await refreshConfig(); await refreshRuntime(); }
  async function ensureSaved(message){
    if(!authState.authenticated || !formDirty) return true;
    if(!(await confirmAction(message || '检测到配置尚未保存，是否先保存？', '保存配置'))) return false;
    await saveConfig();
    toast('配置已自动保存', 'success');
    return true;
  }

  for(const el of Array.from(form.elements || [])){
    if(!el || !el.name) continue;
    const tag = String(el.tagName || '').toUpperCase();
    const type = String(el.type || '').toLowerCase();
    const evt = (tag === 'SELECT' || type === 'checkbox' || type === 'radio') ? 'change' : 'input';
    el.addEventListener(evt, markDirty);
  }
  window.addEventListener('beforeunload', (ev)=>{
    if(!formDirty) return;
    ev.preventDefault();
    ev.returnValue = '';
  });
  async function startRun(){
    const payload = payloadFromForm(false);
    const js = await api('/api/start', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
    await refreshRuntime();
    return js;
  }
  async function stopRun(){ await api('/api/stop', {method:'POST'}); await refreshRuntime(); }

  btnLogin.addEventListener('click', ()=>withBusy(btnLogin, '登录中...', async ()=>{ try{ await api('/api/login', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({password: loginPassword.value})}); loginPassword.value=''; await refreshAuth(); await refreshConfig(); await refreshRuntime(); toast('登录成功', 'success'); }catch(e){ toast('登录失败：' + e.message, 'error'); } }));
  btnLogout.addEventListener('click', ()=>withBusy(btnLogout, '登出中...', async ()=>{ try{ await api('/api/logout', {method:'POST'}); authState.authenticated = false; setAuth(); showRightPanel('logs'); toast('已登出', 'success'); }catch(e){ toast('登出失败：' + e.message, 'error'); } }));
  btnRefresh.addEventListener('click', ()=>withBusy(btnRefresh, '刷新中...', async ()=>{ await refreshAuth().catch(()=>{}); if(authState.authenticated){ await refreshConfig().catch((e)=>toast('读取配置失败：'+e.message, 'error')); } await refreshRuntime(); await refreshStats(); await poll(); toast('页面数据已刷新', 'success'); }));
  btnSave.addEventListener('click', ()=>withBusy(btnSave, '保存中...', async ()=>{ try{ await saveConfig(); showRightPanel('config'); toast('配置已保存', 'success'); }catch(e){ toast('保存失败：'+e.message, 'error'); } }));
  btnSaveReload.addEventListener('click', ()=>withBusy(btnSaveReload, '保存并重载...', async ()=>{ try{ await saveAndReload(); showRightPanel('config'); toast('配置已保存并请求重载', 'success'); }catch(e){ toast('保存并重载失败：'+e.message, 'error'); } }));
  btnStart.addEventListener('click', ()=>withBusy(btnStart, '启动中...', async ()=>{ if(!(await confirmAction('确认启动整理任务吗？', '启动任务'))) { toast('已取消启动', 'warn'); return; } try{ if(!(await ensureSaved('检测到配置已修改，是否先保存后再启动任务？'))) { toast('已取消启动', 'warn'); return; } showRightPanel('logs'); await startRun(); await poll(); toast('已请求启动', 'success'); }catch(e){ toast('启动失败：'+e.message, 'error'); } }));
  btnStop.addEventListener('click', ()=>withBusy(btnStop, '停止中...', async ()=>{ if(!(await confirmAction('确认停止当前整理任务吗？', '停止任务'))) { toast('已取消停止', 'warn'); return; } try{ await stopRun(); toast('已请求停止', 'success'); }catch(e){ toast('停止失败：'+e.message, 'error'); } }));
  btnTestAlist.addEventListener('click', ()=>withBusy(btnTestAlist, '测试中...', async ()=>{ try{ if(!(await ensureSaved('检测到 AList 配置已修改，是否先保存后再测试连接？'))) { toast('已取消测试连接', 'warn'); return; } await testAlist(); }catch(e){ toast('AList 连接测试失败：' + e.message, 'error'); } }));
  btnTestTmdb.addEventListener('click', ()=>withBusy(btnTestTmdb, '验证中...', async ()=>{ try{ if(!(await ensureSaved('检测到 TMDB 配置已修改，是否先保存后再验证 Key？'))) { toast('已取消 TMDB 验证', 'warn'); return; } await testTmdb(); }catch(e){ toast('TMDB 验证失败：' + e.message, 'error'); } }));
  btnClearTmdbCache.addEventListener('click', ()=>withBusy(btnClearTmdbCache, '清理中...', async ()=>{ try{ if(!(await confirmAction('确认清空当前进程中的 TMDB 命中缓存吗？\n清空后下次识别会重新请求 TMDB / AI。', '清 TMDB 缓存'))) { toast('已取消清缓存', 'warn'); return; } const js = await api('/api/tmdb/cache/clear', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({})}); toast(js.message || ('已清除 TMDB 缓存 ' + (js.cleared || 0) + ' 条'), 'success'); }catch(e){ toast('清 TMDB 缓存失败：' + e.message, 'error'); } }));
  btnBrowseTarget.addEventListener('click', ()=>withBusy(btnBrowseTarget, '读取中...', async ()=>{ try{ if(!(await ensureSaved('检测到 AList 或目标目录配置已修改，是否先保存后再浏览目录？'))) { toast('已取消目录浏览', 'warn'); return; } await browseAlist('/'); toast('目录读取完成', 'success'); }catch(e){ toast('AList 目录浏览失败：' + e.message, 'error'); } }));
  btnAlistUp.addEventListener('click', ()=>withBusy(btnAlistUp, '返回中...', async ()=>{ try{ if(!(await ensureSaved('检测到 AList 配置已修改，是否先保存后再读取上级目录？'))) { toast('已取消读取上级目录', 'warn'); return; } await browseAlist(btnAlistUp.dataset.parent || '/'); toast('已返回上级目录', 'success'); }catch(e){ toast('读取上级目录失败：' + e.message, 'error'); } }));
  btnAlistUseCurrent.addEventListener('click', ()=>{ setValue('target_root', alistPath || '/'); markDirty(); toast('已选择目录：' + (alistPath || '/'), 'success'); });
  btnAlistClose.addEventListener('click', ()=>{ alistBrowser.classList.add('hidden'); toast('已关闭目录浏览器', 'info'); });
  btnExpandAll.addEventListener('click', ()=>{ scrollLogsToBottom(); toast('已滚动到最新日志', 'info'); });
  btnCollapseAll.addEventListener('click', ()=>{ if(groupsEl) groupsEl.scrollTop = 0; toast('已滚动到最早日志', 'info'); });
  btnReset.addEventListener('click', ()=>{ fLevel.value='ALL'; fSeason.value=''; fShow.value=''; fKeyword.value=''; rebuild(); toast('过滤条件已重置', 'info'); });
  btnClearLocal.addEventListener('click', async ()=>{ if(!(await confirmAction('确认清空当前页面已缓存的日志吗？\n这不会影响服务端日志。', '清空本页缓存'))) { toast('已取消清空缓存', 'warn'); return; } events=[]; lastId=0; groups.clear(); groupsEl.innerHTML=''; toast('本页缓存已清空', 'success'); });
  if(tabConfig) tabConfig.addEventListener('click', ()=>showRightPanel('config'));
  if(tabLogs) tabLogs.addEventListener('click', ()=>showRightPanel('logs'));
  fLevel.addEventListener('change', rebuild); fSeason.addEventListener('input', ()=>{ clearTimeout(window.__t1); window.__t1=setTimeout(rebuild,180); }); fShow.addEventListener('input', ()=>{ clearTimeout(window.__t2); window.__t2=setTimeout(rebuild,180); }); fKeyword.addEventListener('input', ()=>{ clearTimeout(window.__t3); window.__t3=setTimeout(rebuild,180); });

  (async function boot(){
    try{ await refreshAuth(); }catch(e){}
    if(authState.authenticated){ try{ await refreshConfig(); }catch(e){ toast('读取配置失败：'+e.message); } }
    await refreshRuntime(); await refreshStats(); await poll();
    setInterval(refreshStats, 1800); setInterval(refreshRuntime, 2200); setInterval(poll, 1200);
  })();
})();
</script>
</body>
</html>
"""



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
