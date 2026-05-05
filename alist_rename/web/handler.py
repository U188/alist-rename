"""HTTP request handler factory for the WebUI."""
from __future__ import annotations

import json
import logging
import os
import queue
import re
import time
from dataclasses import asdict
from http import cookies
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Type
from urllib.parse import urlparse, parse_qs

from runtime_config import hash_password, verify_password

from alist_rename.clients.alist import AlistClient
from alist_rename.clients.tmdb import TMDBClient
from alist_rename.web.hub import LogHub
from alist_rename.web.templates import _INDEX_HTML

logger = logging.getLogger("embyrename.logui")

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
