"""Library and series folder discovery."""
from __future__ import annotations

import json
import os
import re
import time
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from alist_rename.clients.ai import AIClient
from alist_rename.clients.alist import AlistClient
AListClient = AlistClient
from alist_rename.common.paths import get_config_dir, join_path, norm_path
from alist_rename.common.text import clean_series_query, normalize_title_for_compare
from alist_rename.media.resolver import levenshtein_ratio
from alist_rename.ops.cleanup import should_skip_misc_folder

logger = logging.getLogger("alist-renamer")

def find_library_root(path: str, roots: List[str]) -> Optional[str]:
    """Find the closest matching library root for a given path."""
    p = norm_path(path)
    best: Optional[str] = None
    for r in roots:
        rr = norm_path(r)
        if p == rr or p.startswith(rr.rstrip("/") + "/"):
            if best is None or len(rr) > len(best):
                best = rr
    return best

def find_top_anchor_root(path: str, roots: List[str]) -> Optional[str]:
    """Find the top-most anchor root (first path segment) among configured roots for a given path."""
    p = norm_path(path)
    anchors: List[str] = []
    seen = set()
    for r in roots:
        rr = norm_path(r)
        parts = [x for x in rr.split("/") if x]
        if not parts:
            continue
        anchor = "/" + parts[0]
        if anchor not in seen:
            seen.add(anchor)
            anchors.append(anchor)
    best: Optional[str] = None
    for anchor in anchors:
        if p == anchor or p.startswith(anchor.rstrip("/") + "/"):
            if best is None or len(anchor) > len(best):
                best = anchor
    return best

def is_season_container_folder(name: str) -> bool:
    return bool(re.search(r"(?i)\bS\d{1,2}\s*-\s*S\d{1,2}\b", name))

def discover_library_roots(
    client: AlistClient,
    root_regex: str = r"^OneDrive-",
    categories_csv: str = "电视剧,动漫",
    ttl_days: int = 7,
) -> List[str]:
    """Backward-compatible wrapper for auto-discovering TV/anime roots."""
    cfg_dir = get_config_dir()
    cache_path = os.path.join(cfg_dir, "roots_cache.json")
    return discover_tv_roots(
        client=client,
        root_regex=root_regex,
        categories_csv=categories_csv,
        max_depth=2,
        cache_path=cache_path,
        cache_ttl_days=ttl_days,
    )

def discover_tv_roots(
    client: AlistClient,
    root_regex: str = r"^OneDrive-",
    categories_csv: str = "电视剧,动漫",
    max_depth: int = 2,
    cache_path: str = "roots_cache.json",
    cache_ttl_days: int = 7,
) -> List[str]:
    """Auto-discover TV/anime library roots with very few AList calls.

    Strategy:
      - list dirs at "/" to find storages (filter by root_regex)
      - for each storage, check 1-2 levels deep to find folders named in categories_csv
        (handles layouts like /OneDrive-xxx/媒体/电视剧)
      - if no storage matches root_regex, gracefully fall back to scanning all top-level
        storages (useful for deployments like /天翼/影视/剧集)
      - expand a few common hub folders such as 媒体/影视/影视一
      - support simple category aliases so "电视剧,动漫" can match folders like 剧集/动漫剧集
    """
    # small local cache to avoid repeated calls
    try:
        if cache_path and os.path.exists(cache_path):
            st = os.stat(cache_path)
            if (time.time() - st.st_mtime) < cache_ttl_days * 86400:
                with open(cache_path, "r", encoding="utf-8") as f:
                    cached = json.load(f) or {}
                key = f"{root_regex}|{categories_csv}"
                if isinstance(cached.get(key), list) and cached[key]:
                    return [norm_path(x) for x in cached[key]]
    except Exception:
        pass

    categories = [c.strip() for c in categories_csv.split(",") if c.strip()]

    alias_map = {
        "电视剧": {"电视剧", "剧集", "连续剧", "电视", "TV", "tv"},
        "动漫": {"动漫", "动漫剧集", "动画", "番剧", "Anime", "anime"},
        "电影": {"电影", "Movie", "movie", "Movies", "movies"},
        "综艺": {"综艺", "Variety", "variety"},
        "纪录片": {"纪录片", "Documentary", "documentary"},
    }
    category_aliases: Dict[str, set] = {}
    for c in categories:
        aliases = set(alias_map.get(c, {c}))
        aliases.add(c)
        category_aliases[c] = {a.strip() for a in aliases if str(a).strip()}

    def match_category(dir_name: str) -> bool:
        name = (dir_name or "").strip()
        if not name:
            return False
        for cat, aliases in category_aliases.items():
            if name == cat or name in aliases:
                return True
            # tolerate merged naming like 动漫剧集 / 日韩动漫
            for a in aliases:
                if a and (a in name or name in a):
                    return True
        return False

    try:
        root_re = re.compile(root_regex)
    except re.error:
        root_re = re.compile(r"^OneDrive-")

    storage_dirs = client.list_dirs_only("/")
    storages = [item for item in storage_dirs if root_re.search(str(item.get("name") or ""))]
    if not storages:
        # Fallback for non-OneDrive layouts such as /天翼
        storages = [item for item in storage_dirs if str(item.get("name") or "").strip()]
        logger.info("[DISCOVER] no storage matched root_regex=%s; fallback to all top-level storages=%s", root_regex, [str(x.get("name") or "") for x in storages])
    found: List[str] = []

    # BFS to limited depth per storage
    for s in storages:
        base = norm_path(str(s.get("path") or join_path("/", str(s.get("name") or ""))))
        if not base or base == "/":
            continue
        queue: List[Tuple[str, int]] = [(base, 0)]
        seen: set = set()
        while queue:
            cur, depth = queue.pop(0)
            if cur in seen:
                continue
            seen.add(cur)
            try:
                dirs = client.list_dirs_only(cur)
            except Exception:
                logger.exception("[DISCOVER] list_dirs_only failed while scanning cur=%s depth=%s", cur, depth)
                dirs = []
            for d in dirs:
                d_name = str(d.get("name") or "").strip()
                d_path = norm_path(str(d.get("path") or join_path(cur, d_name))) if d_name else ""
                if match_category(d_name) and d_path:
                    found.append(d_path)
            if depth < max_depth:
                for d in dirs:
                    d_name = str(d.get("name") or "").strip()
                    d_path = norm_path(str(d.get("path") or join_path(cur, d_name))) if d_name else ""
                    if d_path:
                        queue.append((d_path, depth + 1))

    # de-duplicate, keep stable order
    uniq: List[str] = []
    for p in found:
        p = norm_path(p)
        if p not in uniq:
            uniq.append(p)

    # write cache
    try:
        if cache_path:
            existing = {}
            if os.path.exists(cache_path):
                with open(cache_path, "r", encoding="utf-8") as f:
                    existing = json.load(f) or {}
            key = f"{root_regex}|{categories_csv}"
            existing[key] = uniq
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(existing, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    return uniq

def resolve_series_folders_by_keyword(
    client: AlistClient,
    roots: List[str],
    keyword: str,
    skip_dir_regex: str,
) -> List[str]:
    """Resolve series folders from a user keyword.

    Priority:
      1) If keyword looks like an absolute path, treat it as a direct folder path (no search).
      2) Try AList /api/fs/search (fast when you have index enabled).
      3) Fallback to recursively list a limited depth under each root and fuzzy-match candidate folders.

    This fallback is designed for the common case where AList search is unavailable (no index).
    It walks a small bounded subtree so user-provided roots like /天翼/视频 can still cover nested show folders.
    """

    kw_raw = (keyword or "").strip()
    if not kw_raw:
        return []

    # ---- 1) Direct path mode ----
    # Users may pass a full AList path like: /OneDrive-xxx/电视剧/他为什么依然单身
    if kw_raw.startswith("/") and "/" in kw_raw[1:]:
        return [kw_raw.rstrip("/")]

    def norm(s: str) -> str:
        s = (s or "").lower().strip()
        # remove common separators/brackets/spaces to improve match robustness
        s = re.sub(r"[\s\-_.·•]+", "", s)
        s = re.sub(r"[\[\]【】()（）{}<>《》]", "", s)
        return s

    kw = norm(kw_raw)

    # ---- 2) Search API (requires index; may return empty if disabled) ----
    hits: List[str] = []
    for r in roots:
        try:
            results = client.search(r, kw_raw, scope=1, per_page=100)
        except Exception:
            results = []
        for item in results:
            name = str(item.get("name") or "").strip()
            parent = str(item.get("parent") or r).strip()
            if not name:
                continue
            if should_skip_misc_folder(name, skip_dir_regex):
                continue
            full = join_path(parent, name)
            if full not in hits:
                hits.append(full)

    if hits:
        return hits

    def _dir_name_and_path(item: Any, base: str) -> Tuple[str, str]:
        if isinstance(item, str):
            name = item.strip()
            path = join_path(base, name) if name else ""
            return name, path
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
            raw_path = str(item.get("path") or "").strip()
            path = norm_path(raw_path) if raw_path else (join_path(base, name) if name else "")
            return name, path
        name = str(item or "").strip()
        path = join_path(base, name) if name else ""
        return name, path

    # ---- 3) Fallback: recursively scan a limited depth under the provided roots ----
    fallback: List[str] = []
    max_scan_depth = 4
    for r in roots:
        queue: List[Tuple[str, int]] = [(norm_path(r), 0)]
        seen: set = set()
        while queue:
            cur, depth = queue.pop(0)
            if not cur or cur in seen:
                continue
            seen.add(cur)
            try:
                dirs = client.list_dirs_only(cur)
            except Exception:
                dirs = []
            for d in dirs:
                name, full = _dir_name_and_path(d, cur)
                if not name or not full:
                    continue
                if should_skip_misc_folder(name, skip_dir_regex):
                    continue
                n = norm(name)
                if n and (kw in n or n in kw):
                    if full not in fallback:
                        fallback.append(full)
                if depth < max_scan_depth:
                    queue.append((full, depth + 1))

    return fallback

def search_series_dirs(client: "AListClient", roots: List[str], keyword: str, skip_dir_regex: Optional[str] = None) -> List[str]:
    """Fast lookup of series folders by keyword, using AList search (no full traversal)."""
    return resolve_series_folders_by_keyword(client, roots, keyword, skip_dir_regex=skip_dir_regex)

def ai_choose_series_path(ai: AIClient, keyword: str, candidates: List[str]) -> Optional[str]:
    """Ask AI to pick the best path from candidates (optional)."""
    system = "You pick the best matching series folder path. Output JSON only."
    user = (
        "Pick the best matching TV series folder path for this keyword. "
        "Return JSON: {\"path\": string|null}.\n\n"
        f"keyword: {keyword}\n"
        f"candidates: {candidates[:12]}"
    )
    js = ai.chat_json(system, user)
    if not js:
        return None
    p = js.get("path")
    if isinstance(p, str):
        p = p.strip()
        return p or None
    return None

def pick_series_dirs(
    client: "AListClient",
    roots: List[str],
    keyword: str,
    skip_dir_regex: Optional[str] = None,
    ai: Optional[AIClient] = None,
) -> List[str]:
    """Pick best-matching series dirs for a keyword, avoiding full traversal.

    Strategy:
    - Use AList search to get candidate series folders.
    - If multiple, pick the best by fuzzy score; optionally ask AI to choose.
    """
    hits = resolve_series_folders_by_keyword(client, roots, keyword, skip_dir_regex=skip_dir_regex)
    if not hits:
        return []
    if len(hits) == 1:
        return hits

    # Heuristic pick by similarity of leaf folder name
    def _norm_name(p: str) -> str:
        name = Path(p).name
        name = re.sub(r"\[[^\]]+\]", " ", name)
        name = re.sub(r"\([^\)]+\)", " ", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name

    scored = sorted(hits, key=lambda p: levenshtein_ratio(_norm_name(p), keyword), reverse=True)

    if ai is not None:
        chosen = ai_choose_series_path(ai, keyword, scored)
        if chosen and chosen in hits:
            return [chosen]

    return [scored[0]]

__all__ = [name for name in globals() if not name.startswith("__")]
