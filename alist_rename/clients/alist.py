"""AList API client."""
import json
import logging
import time
from time import sleep
from typing import Any, Callable, Dict, List, Optional

import requests

from alist_rename.config import CURRENT_RUNTIME_CONFIG
from alist_rename.common.paths import norm_path
from alist_rename.common.rate_limit import RateLimiter
from alist_rename.media.models import DirEntry

logger = logging.getLogger("embyrename")

class AlistClient:
    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        otp_code: Optional[str] = None,
        sleep: float = 0.8,
        timeout: float = 30.0,
        verify_tls: bool = True,
        on_token_refresh: Optional[Callable[[str], None]] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.username = username
        self.password = password
        self.otp_code = otp_code
        self.timeout = timeout
        self.verify_tls = verify_tls
        self.on_token_refresh = on_token_refresh
        self.rl_read = RateLimiter(float(CURRENT_RUNTIME_CONFIG.get("alist_sleep_read", sleep)))
        self.rl_write = RateLimiter(float(CURRENT_RUNTIME_CONFIG.get("alist_sleep_write", max(1.2, float(sleep)))))
        self.retries = int(CURRENT_RUNTIME_CONFIG.get("alist_retries", 5))
        self.retry_base = float(CURRENT_RUNTIME_CONFIG.get("alist_retry_base", 0.8))
        self.retry_max = float(CURRENT_RUNTIME_CONFIG.get("alist_retry_max", 10.0))

    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = self.token
        return h

    def _persist_token(self, token: str):
        self.token = token
        cb = self.on_token_refresh
        if not cb:
            return
        try:
            cb(token)
        except Exception:
            logger.exception("[ALIST] token refresh callback failed")

    def login_if_needed(self, force: bool = False):
        if self.token and not force:
            return
        if not (self.username and self.password):
            raise ValueError("Need either ALIST_TOKEN or ALIST_USER+ALIST_PASS.")
        self.rl_read.wait()
        url = self.base_url + "/api/auth/login"
        payload: Dict[str, Any] = {"username": self.username, "password": self.password}
        if self.otp_code:
            payload["otp_code"] = self.otp_code
        auth_mode = "password+otp" if self.otp_code else "password"
        started = time.monotonic()
        logger.info("[ALIST] login start base=%s auth=%s user=%s timeout=%ss verify_tls=%s", self.base_url, auth_mode, self.username, self.timeout, self.verify_tls)
        r = requests.post(url, headers={"Content-Type": "application/json"}, json=payload, timeout=self.timeout, verify=self.verify_tls)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 200:
            raise RuntimeError(f"login failed: {data}")
        token = str(((data.get("data") or {}).get("token") or "")).strip()
        if not token:
            raise RuntimeError(f"login returned empty token: {data}")
        self._persist_token(token)
        logger.info("[ALIST] login success base=%s auth=%s user=%s elapsed=%.3fs token_len=%s", self.base_url, auth_mode, self.username, time.monotonic() - started, len(token))


    def post(self, path: str, payload: Dict[str, Any], kind: str = "read") -> Dict[str, Any]:
        """POST to AList API with rate limit + retries.

        kind: 'read' (list/search/get) or 'write' (rename/move/mkdir).
        """
        self.login_if_needed()
        rl = self.rl_write if kind == "write" else self.rl_read
        last_err: Exception | None = None
        relogin_attempted = False
        auth_mode = "token" if self.token else ("password" if self.username and self.password else "anonymous")
        safe_payload = dict(payload or {})
        if "password" in safe_payload:
            safe_payload["password"] = "***" if safe_payload.get("password") else ""
        for attempt in range(max(1, self.retries)):
            started = time.monotonic()
            try:
                rl.wait()
                url = self.base_url + path
                logger.info("[ALIST] request start path=%s kind=%s attempt=%s/%s auth=%s payload=%s", path, kind, attempt + 1, max(1, self.retries), auth_mode, safe_payload)
                r = requests.post(url, headers=self._headers(), json=payload, timeout=self.timeout, verify=self.verify_tls)
                elapsed = time.monotonic() - started
                logger.info("[ALIST] request response path=%s kind=%s attempt=%s status=%s elapsed=%.3fs bytes=%s", path, kind, attempt + 1, r.status_code, elapsed, len(r.text or ""))
                # Retry on transient HTTP
                if r.status_code in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                if r.status_code == 401 and self.username and self.password and not relogin_attempted:
                    logger.warning("[ALIST] HTTP 401 for %s; retrying with password login", path)
                    self.token = None
                    self.login_if_needed(force=True)
                    relogin_attempted = True
                    auth_mode = "password"
                    continue
                r.raise_for_status()
                data = r.json()
                if data.get("code") != 200:
                    # Provider transient errors often surface as 500 with message
                    msg = str(data)
                    if data.get("code") in (429, 500, 502, 503, 504):
                        raise RuntimeError(f"AList transient {path}: {msg}")
                    if data.get("code") == 401 and self.username and self.password and not relogin_attempted:
                        logger.warning("[ALIST] API token invalid for %s; retrying with password login", path)
                        self.token = None
                        self.login_if_needed(force=True)
                        relogin_attempted = True
                        auth_mode = "password"
                        continue
                    raise RuntimeError(f"AList API error {path}: {data}")
                logger.info("[ALIST] request success path=%s kind=%s attempt=%s code=%s keys=%s", path, kind, attempt + 1, data.get("code"), sorted(list(data.keys())))
                return data
            except Exception as e:
                last_err = e
                logger.warning("[ALIST] request failed path=%s kind=%s attempt=%s/%s auth=%s elapsed=%.3fs err=%s", path, kind, attempt + 1, max(1, self.retries), auth_mode, time.monotonic() - started, e)
                if attempt >= max(1, self.retries) - 1:
                    break
                # exponential backoff
                sleep = min(self.retry_max, self.retry_base * (2 ** attempt))
                logger.info("[ALIST] retry sleep path=%s kind=%s attempt=%s sleep=%.3fs", path, kind, attempt + 1, sleep)
                time.sleep(sleep)
        raise RuntimeError(str(last_err) if last_err else f"AList API error {path}")

    def list_dir(self, path: str, refresh: bool = True, per_page: int = 200, max_pages: int = 200) -> List[DirEntry]:
        """List a directory (files + dirs) with pagination.

        Notes:
        - AList /api/fs/list is paginated by (page, per_page). Using per_page=0 can cause
          inconsistent behavior on some providers; we always use a positive per_page.
        - To reduce load, we only set refresh=True for the first page; subsequent pages use refresh=False.
        - OneDrive providers may throw transient errors when refresh is on; default is gated by ALIST_REFRESH=1.
        """
        path = norm_path(path)
        refresh = bool(refresh) and bool(CURRENT_RUNTIME_CONFIG.get("alist_refresh", False))
        logger.info("[ALIST] list_dir start path=%s refresh=%s per_page=%s max_pages=%s", path, refresh, per_page, max_pages)
        started = time.monotonic()
        out: List[DirEntry] = []
        page = 1
        total = None
        while True:
            try:
                data = self.post(
                    "/api/fs/list",
                    {
                        "path": path,
                        "password": "",
                        "page": page,
                        "per_page": per_page,
                        "refresh": bool(refresh) if page == 1 else False,
                    },
                    kind="read",
                )
            except Exception as e:
                logger.exception("[ALIST] list_dir failed path=%s page=%s per_page=%s refresh=%s", path, page, per_page, bool(refresh) if page == 1 else False)
                raise
            d = data.get("data") or {}
            content = d.get("content") or []
            if total is None:
                try:
                    total = int(d.get("total") or 0)
                except Exception:
                    total = 0
            logger.info("[ALIST] list_dir page path=%s page=%s content=%s total=%s accumulated=%s", path, page, len(content), total, len(out) + len(content))
            for it in content:
                size = it.get("size")
                try:
                    size = int(size) if size is not None else None
                except Exception:
                    size = None
                hash_info = it.get("hash_info") or it.get("hashinfo") or it.get("hash") or it.get("sign") or None
                if isinstance(hash_info, (dict, list)):
                    try:
                        hash_info = json.dumps(hash_info, ensure_ascii=False, sort_keys=True)
                    except Exception:
                        hash_info = str(hash_info)
                elif hash_info is not None:
                    hash_info = str(hash_info)
                out.append(DirEntry(
                    name=it.get("name", ""),
                    is_dir=bool(it.get("is_dir")),
                    size=size,
                    hash_info=hash_info,
                ))
            if not content:
                break
            if total and len(out) >= total:
                break
            page += 1
            if page > max_pages:
                logger.warning("[ALIST] list_dir reached max_pages path=%s max_pages=%s current_count=%s", path, max_pages, len(out))
                break
        dir_count = sum(1 for e in out if e.is_dir)
        logger.info("[ALIST] list_dir done path=%s entries=%s dirs=%s elapsed=%.3fs", path, len(out), dir_count, time.monotonic() - started)
        return out

    def list_dirs_only(self, path: str) -> List[Dict[str, Any]]:
        """Return direct child directories with both display name and full path."""
        path = norm_path(path)
        logger.info("[ALIST] list_dirs_only start path=%s", path)
        started = time.monotonic()
        # Newer AList versions support /api/fs/dirs.
        # Response shape is not stable across versions:
        #   - {data: {content: [...]}}
        #   - {data: [...]}  # simple list of names/objects
        # Some older builds (or reverse proxies) may not expose it; in that case,
        # fall back to /api/fs/list and filter directories.
        try:
            data = self.post("/api/fs/dirs", {"path": path, "password": ""}, kind="read")
            raw = data.get("data")
            if isinstance(raw, dict):
                items = raw.get("content") or []
            elif isinstance(raw, list):
                items = raw
            else:
                items = []
            dirs: List[Dict[str, Any]] = []
            for it in items:
                if isinstance(it, dict):
                    n = str(it.get("name") or "").strip()
                    full = norm_path(it.get("path") or f"{path.rstrip('/')}/{n}") if n else ''
                else:
                    n = str(it or '').strip()
                    full = norm_path(f"{path.rstrip('/')}/{n}") if n else ''
                if n:
                    dirs.append({"name": n, "path": full or '/'})
            logger.info("[ALIST] list_dirs_only done path=%s source=dirs_api count=%s elapsed=%.3fs", path, len(dirs), time.monotonic() - started)
            return dirs
        except Exception:
            logger.exception("[ALIST] list_dirs_only failed path=%s via /api/fs/dirs; fallback to list_dir", path)
            # Fallback: list directory, but do not refresh to reduce load/rate-limit risk.
            entries = self.list_dir(path, refresh=False)
            dirs = [
                {"name": e.name, "path": norm_path(f"{path.rstrip('/')}/{e.name}") or '/'}
                for e in entries if e.is_dir and e.name
            ]
            logger.info("[ALIST] list_dirs_only done path=%s source=list_dir_fallback count=%s elapsed=%.3fs", path, len(dirs), time.monotonic() - started)
            return dirs

    def search(self, parent: str, keywords: str, scope: int = 1, per_page: int = 200, page: int = 1) -> List[Dict[str, Any]]:
        """Server-side search. Returns raw items from /api/fs/search."""
        parent = norm_path(parent)
        payload = {
            "parent": parent,
            "keywords": keywords,
            "scope": int(scope),
            "page": int(page),
            "per_page": int(per_page),
            "password": "",
        }
        data = self.post("/api/fs/search", payload, kind="read")
        return (data.get("data") or {}).get("content") or []

    def mkdir(self, path: str):
        path = norm_path(path)
        self.post("/api/fs/mkdir", {"path": path}, kind="write")

    def rename(self, path: str, new_name: str):
        path = norm_path(path)
        self.post("/api/fs/rename", {"path": path, "name": new_name}, kind="write")

    def move(self, src_dir: str, dst_dir: str, names: List[str]):
        src_dir = norm_path(src_dir)
        dst_dir = norm_path(dst_dir)
        if not names:
            return
        self.post("/api/fs/move", {"src_dir": src_dir, "dst_dir": dst_dir, "names": names}, kind="write")


    def remove(self, dir_path: str, names: List[str]):
        """Remove files/folders under a directory.

        NOTE: AList/OpenList commonly exposes /api/fs/remove with payload:
          {"dir":"/path","names":["a","b"]}
        If the backend does not support it, we log and continue (best-effort).
        """
        dir_path = norm_path(dir_path)
        if not names:
            return
        try:
            self.post("/api/fs/remove", {"dir": dir_path, "names": names}, kind="write")
        except Exception as e:
            # Don't crash the whole run for cleanup failures.
            logger.warning("[WARN] remove failed for %s/%s : %s", dir_path, names, e)

__all__ = ["AlistClient"]
