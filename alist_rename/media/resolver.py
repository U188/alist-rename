"""Series metadata resolution helpers."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple

from logui import LogHub
from alist_rename.clients.ai import AIClient
from alist_rename.clients.alist import AlistClient
from alist_rename.clients.tmdb import TMDBClient
from alist_rename.common.paths import join_path, norm_path
from alist_rename.common.text import (
    bool_env, clean_series_query, extract_year_hint, normalize_spaces,
    normalize_title_for_compare, to_halfwidth,
)
from alist_rename.media.models import SeriesMeta
from alist_rename.ops.filesystem import ensure_dir
from alist_rename.media.parse import (
    is_special_episode_name, parse_date_key, parse_episode_from_name,
    parse_qishu_and_part, parse_season_from_text,
)
from alist_rename.ops.cleanup import MISC_DIR_NAMES, should_skip_misc_folder

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".ts", ".m2ts", ".webm", ".rmvb", ".mpg", ".mpeg", ".m4v"}
DEFAULT_SKIP_DIR_REGEX = r"(?i)(^|/)(@eaDir|#recycle|\$RECYCLE\.BIN|System Volume Information)(/|$)"
CATEGORY_CONTAINER_NAMES = {"电视剧", "剧集", "连续剧", "动漫", "动画", "纪录片", "综艺", "电影", "华语", "欧美", "日韩", "日本", "韩国", "中国", "大陆", "香港", "台湾", "美国", "英国", "泰国", "海外", "其他"}
DEFAULT_CATEGORY_REGION_MAP = {
    "国漫": "中国", "国产动漫": "中国", "动漫": "日本", "日漫": "日本",
    "纪录片": "纪录片", "综艺": "综艺",
    "国产剧": "中国", "华语剧": "中国", "港剧": "香港", "台剧": "台湾",
    "日剧": "日本", "韩剧": "韩国", "美剧": "美国", "英剧": "英国", "泰剧": "泰国",
    "欧美剧": "欧美", "海外剧": "海外", "其他": "其他",
}

def looks_like_show_folder_name(name: str) -> bool:
    """Heuristic: determine if a folder likely represents a standalone show (not a season folder).

    Examples:
      - "法医秦明2清道夫(2018)全20集"  -> True
      - "龙岭迷窟 (2020) 4K"          -> True
      - "第二季 (2017) 全42集 1080P"  -> False (season folder)
      - "S01" / "Season 1"          -> False (season folder)
      - "4K" / "1080P"             -> False (quality folder)
    """
    if not name:
        return False

    name = to_halfwidth(name).strip()

    if name in CATEGORY_CONTAINER_NAMES:
        return False

    # Packaging year folders (e.g. variety show "2024") are not shows.
    if re.fullmatch(r"20\d{2}", name) or re.fullmatch(r"20\d{2}\s*年", name):
        return False

    # Avoid counting season folders as "show" folders.
    if parse_season_from_text(name) is not None:
        return False

    # Skip obvious misc folders.
    if should_skip_misc_folder(name, DEFAULT_SKIP_DIR_REGEX):
        return False

    # If there's an explicit year, it's very likely a show folder.
    if extract_year_hint(name) is not None:
        return True

    # Full-episode collections in the name are also a strong indicator.
    if re.search(r"全\s*\d+\s*(集|话|回)", name):
        return True

    # Common title patterns: "标题 (YYYY)" with other tags.
    if re.search(r"[（(]\s*(19\d{2}|20\d{2})\s*[)）]", name):
        return True

    return False

def is_same_show_container_folder(child_name: str, parent_title: str) -> bool:
    """Return True when `child_name` looks like it is just another packaging folder for the SAME show.

    Important: be strict. Parent title being a substring of child title is NOT enough ("法医秦明" vs "法医秦明2...").
    """
    # Strip common packaging tokens that often appear in folder names for the SAME show,
    # e.g. "全集", "全20集", "共16集", etc.
    packaging_re = re.compile(
        r"(全集|全\s*\d+\s*(集|话|回)|共\s*\d+\s*(集|话|回)|\d+\s*(集|话|回))",
        flags=re.IGNORECASE,
    )

    def _norm_same_show(t: str) -> str:
        t = clean_series_query(t)
        t = packaging_re.sub("", t)
        # Remove explicit years regardless of punctuation/word boundaries (e.g. "(2016)", "2016").
        t = re.sub(r"(19\d{2}|20\d{2})", "", t)
        # Remove a few extra common suffixes.
        t = re.sub(r"(全套|全季|完整版|完结|完結|complete)", "", t, flags=re.IGNORECASE)
        t = normalize_title_for_compare(t)
        t = re.sub(r"(19\d{2}|20\d{2})", "", t)
        return t

    c = _norm_same_show(child_name)
    p = _norm_same_show(parent_title)
    if not c or not p:
        return False
    if c == p:
        return True
    # Guard: sequel / spin-off titles that merely share a parent prefix are NOT the same show.
    if c.startswith(p) or p.startswith(c):
        longer = c if len(c) >= len(p) else p
        shorter = p if longer == c else c
        tail = longer[len(shorter):]
        if tail and re.search(r"[A-Za-z\u4e00-\u9fff0-9]", tail):
            return False
    # Allow only very high similarity after normalization.
    return levenshtein_ratio(c, p) >= 0.93

def parse_boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")

def parse_category_region_map(raw: Any) -> Dict[str, List[str]]:
    if isinstance(raw, dict):
        out: Dict[str, List[str]] = {}
        for k, v in raw.items():
            key = str(k or "").strip()
            if not key:
                continue
            vals = v if isinstance(v, list) else str(v or "").split(",")
            cleaned = [str(x).strip() for x in vals if str(x).strip()]
            if cleaned:
                out[key] = cleaned
        return out
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return parse_category_region_map(obj)
    except Exception:
        pass
    out: Dict[str, List[str]] = {}
    for part in text.split(";"):
        part = part.strip()
        if not part or ":" not in part:
            continue
        cat, vals = part.split(":", 1)
        cat = cat.strip()
        regions = [x.strip() for x in vals.split(",") if x.strip()]
        if cat and regions:
            out[cat] = regions
    return out

def ensure_organize_tree(client: 'AlistClient', target_root: str, mapping: Dict[str, List[str]], dry_run: bool, log: List[str]) -> Dict[str, List[str]]:
    root = norm_path(target_root)
    built: Dict[str, List[str]] = {}
    root_exists = True
    try:
        client.list_dir(root)
    except Exception:
        root_exists = False
        if dry_run:
            log.append(f"[DRY] mkdir {root}")
    for category, regions in mapping.items():
        cat_dir = ensure_dir(client, root, category, dry_run, log, assume_exists=(dry_run and not root_exists))
        built[category] = []
        for region in regions:
            reg_dir = ensure_dir(client, cat_dir, region, dry_run, log, assume_exists=dry_run)
            built[category].append(reg_dir)
    return built

def _norm_region_token(text: Any) -> str:
    s = str(text or "").strip().lower()
    s = s.replace(" ", "")
    return s

def infer_category_region_from_tmdb(details: Dict[str, Any], mapping: Dict[str, List[str]], title_hint: str = "") -> Tuple[Optional[str], Optional[str]]:
    category: Optional[str] = None
    region: Optional[str] = None

    genre_names = {
        str(x.get("name") or "").strip().lower()
        for x in (details.get("genres") or [])
        if isinstance(x, dict) and str(x.get("name") or "").strip()
    }
    origin_countries = []
    for key in ("origin_country", "production_countries"):
        vals = details.get(key) or []
        if isinstance(vals, list):
            for item in vals:
                if isinstance(item, str) and item.strip():
                    origin_countries.append(item.strip())
                elif isinstance(item, dict):
                    nm = str(item.get("iso_3166_1") or item.get("name") or "").strip()
                    if nm:
                        origin_countries.append(nm)

    normalized_title_hint = clean_series_query(title_hint or "").lower()
    strong_category_keywords = {
        "动漫": ("动漫", "动画", "番剧", "番", "国漫", "日漫", "美漫", "anime", "animation"),
        "动画": ("动漫", "动画", "番剧", "番", "国漫", "日漫", "美漫", "anime", "animation"),
        "纪录片": ("纪录片", "紀錄片", "documentary", "docu"),
        "综艺": ("综艺", "綜藝", "真人秀", "脱口秀", "talk show", "reality show", "variety"),
    }
    for cat, keywords in strong_category_keywords.items():
        if cat in mapping and normalized_title_hint and any(k in normalized_title_hint for k in keywords):
            category = cat
            break

    category_aliases = {
        "纪录片": {"documentary"},
        "动漫": {"animation", "anime"},
        "动画": {"animation", "anime"},
        "综艺": {"reality", "talk", "news"},
        "剧集": {"drama", "comedy", "crime", "mystery", "sci-fi & fantasy", "action & adventure", "war & politics", "family", "kids"},
    }
    if category is None:
        for cat, aliases in category_aliases.items():
            if cat in mapping and (genre_names & aliases):
                category = cat
                break
    if category is None:
        if "动漫" in mapping and any(x in genre_names for x in ("animation", "anime")):
            category = "动漫"
        elif "纪录片" in mapping and "documentary" in genre_names:
            category = "纪录片"
        elif "综艺" in mapping and any(x in genre_names for x in ("reality", "talk", "news")):
            category = "综艺"
        elif "剧集" in mapping:
            category = "剧集"
        elif mapping:
            category = next(iter(mapping.keys()))

    region_aliases = {
        "中国大陆": {"cn", "china", "中国", "中国大陆", "prc", "people'srepublicofchina", "people's republic of china"},
        "大陆": {"cn", "china", "中国", "中国大陆", "大陆", "内地", "prc", "people'srepublicofchina", "people's republic of china"},
        "中国香港": {"hk", "hongkong", "hong kong", "中国香港", "香港"},
        "中国台湾": {"tw", "taiwan", "中国台湾", "台湾"},
        "港台": {"hk", "hongkong", "hong kong", "中国香港", "香港", "tw", "taiwan", "中国台湾", "台湾", "港澳台", "港台"},
        "日本": {"jp", "japan", "日本"},
        "韩国": {"kr", "korea", "southkorea", "south korea", "republicofkorea", "republic of korea", "韩国"},
        "日韩": {"jp", "japan", "日本", "kr", "korea", "southkorea", "south korea", "republicofkorea", "republic of korea", "韩国", "日韩"},
        "美国": {"us", "usa", "unitedstates", "united states", "美国"},
        "英国": {"gb", "uk", "britain", "unitedkingdom", "united kingdom", "英国"},
        "欧美": {"us", "usa", "unitedstates", "united states", "美国", "gb", "uk", "britain", "unitedkingdom", "united kingdom", "英国", "eu", "europe", "欧洲", "法国", "德国", "西班牙", "意大利", "欧美"},
    }
    normalized_origins = {_norm_region_token(x) for x in origin_countries if str(x or "").strip()}
    candidate_regions = mapping.get(category or "", []) if category else []
    for reg in candidate_regions:
        aliases = region_aliases.get(reg, {reg})
        aliases = {_norm_region_token(x) for x in aliases}
        if normalized_origins & aliases:
            region = reg
            break
    if region is None and candidate_regions:
        region = candidate_regions[-1]
    return category, region

def pick_organized_destination(series_path: str, organize_root: str, mapping: Dict[str, List[str]], meta: Optional[SeriesMeta] = None) -> str:
    organize_root = norm_path(organize_root)
    if meta and meta.category:
        if meta.region:
            return join_path(join_path(organize_root, meta.category), meta.region)
        return join_path(organize_root, meta.category)
    sp = norm_path(series_path)
    parent_name = Path(sp).parent.name
    leaf_name = Path(sp).name
    parts = [x for x in [parent_name, leaf_name] if x]
    for category, regions in mapping.items():
        if category in parts:
            for region in regions:
                if region in parts:
                    return join_path(join_path(organize_root, category), region)
            if regions:
                return join_path(join_path(organize_root, category), regions[-1])
            return join_path(organize_root, category)
    return organize_root

def extract_english_title_from_filename(name: str) -> Optional[str]:
    """Try to extract a likely English title prefix from a filename.

    Best-effort heuristic. Works with patterns like:
      - Beyond.Evil.S01E01...
      - Star.Wars.Andor.S01E01...
      - Foo 1x02 ...
      - Foo.E02...
    It is intentionally conservative (returns None if unsure).
    """
    stem = os.path.splitext(name)[0]

    # Strip bracketed tags early
    stem = re.sub(r"[\[\(【].*?[\]\)】]", " ", stem)

    # Take prefix before common episode markers (no \b because CJK + 'S1E1' has no word boundary)
    m = re.search(
        r"^(.*?)(?:S\d{1,2}\s*E\d{1,3}|\d{1,2}\s*[xX]\s*\d{1,3}|(?:^|[\s._\-])E\d{1,3}\b)",
        stem,
        re.IGNORECASE,
    )
    if m:
        stem = m.group(1)

    stem = stem.replace(".", " ").replace("_", " ").replace("-", " ")
    stem = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", stem)
    stem = re.sub(
        r"\b(720p|1080p|2160p|4k|web[- ]?dl|webrip|blu[- ]?ray|hdr|dv|dovi|atmos|ddp|aac|dts|truehd|x264|x265|h\.?264|h\.?265)\b",
        " ",
        stem,
        flags=re.IGNORECASE,
    )
    stem = re.sub(r"\s+", " ", stem).strip()
    if not stem:
        return None

    # Keep only ASCII to avoid mixing with CJK titles
    ascii_only = re.sub(r"[^A-Za-z0-9 ']+", " ", stem)
    ascii_only = re.sub(r"\s+", " ", ascii_only).strip()
    if not ascii_only:
        return None

    # English-ish heuristic
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ']+", ascii_only) and len(ascii_only) >= 3:
        return ascii_only
    return None

def gather_series_context(client: 'AlistClient', series_path: str, skip_dir_regex: str) -> Dict[str, Any]:
    """Collect extra hints (english title, episode count) from filenames to help TMDB matching.

    Cheap on purpose: reads the current folder and, if needed, a couple of immediate subfolders
    (so shows that keep episodes under 'S01' still get good context).
    """
    ctx: Dict[str, Any] = {
        "year_hint": extract_year_hint(os.path.basename(series_path)),
        "english_title": None,
        "sample_files": [],
        "max_episode": None,
        "episode_max": None,  # backward compat
        "episode_file_count": None,
    }

    try:
        entries = client.list_dir(series_path, refresh=False)
    except Exception:
        return ctx

    skip_re = re.compile(skip_dir_regex) if skip_dir_regex else None

    # collect some video filenames from root
    video_names: List[str] = []
    for e in entries:
        if not e.is_dir and os.path.splitext(e.name)[1].lower() in VIDEO_EXTS:
            video_names.append(e.name)

    # If root has no videos, peek into a few child folders (including season folders)
    if not video_names:
        subdirs = []
        for e in entries:
            if not e.is_dir:
                continue
            if e.name.lower() in MISC_DIR_NAMES:
                continue
            if skip_re and skip_re.search(e.name):
                continue
            subdirs.append(e)

        # prefer season-like dirs first, then by name
        subdirs.sort(key=lambda d: (parse_season_from_text(d.name) is None, d.name))
        for d in subdirs[:3]:
            try:
                sub_entries = client.list_dir(os.path.join(series_path, d.name), refresh=False)
            except Exception:
                continue
            for se in sub_entries:
                if not se.is_dir and os.path.splitext(se.name)[1].lower() in VIDEO_EXTS:
                    video_names.append(se.name)
            if video_names:
                break

    ctx["sample_files"] = video_names[:8]

    # English title (best-effort) from sample files (choose the longest plausible one)
    best_eng: Optional[str] = None
    for n in ctx["sample_files"]:
        eng = extract_english_title_from_filename(n)
        if eng and (best_eng is None or len(eng) > len(best_eng)):
            best_eng = eng
    ctx["english_title"] = best_eng

    # Episode statistics (best-effort)
    eps: List[int] = []
    for n in video_names[:200]:
        _s, e, _has, _tail = parse_episode_from_name(n)
        if e is not None:
            eps.append(int(e))

    if eps:
        ctx["max_episode"] = max(eps)
        ctx["episode_max"] = ctx["max_episode"]
        ctx["episode_file_count"] = len(set(eps))

    return ctx

def infer_variety_and_special_episodes(
    client: AlistClient,
    scan_dirs: List[str],
    incoming_scan_season_hints: Dict[str, int],
    default_season: int,
) -> Dict[Tuple[str, str], Tuple[int, int, bool]]:
    """Infer (season, episode, is_special) for video files that don't contain episode numbers.

    - If filename has only a date (YYYYMMDD / YYYY-MM-DD), assign episodes sequentially by date.
    - If filename has '第X期上/下', assign sequential episodes by (X, part).
    - Specials like '抢先看/花絮/特辑' go to season 0 (Specials).
    """
    plans: Dict[Tuple[str, str], Tuple[int, int, bool]] = {}

    # Gather candidates per season
    cand_by_season: Dict[int, List[Tuple[Tuple, str, str, bool]]] = {}
    used_by_season: Dict[int, set] = {}

    for scan_dir in scan_dirs:
        scan_basename = os.path.basename(scan_dir).strip()
        scan_season_hint = incoming_scan_season_hints.get(scan_dir) or parse_season_from_text(scan_basename)
        season_guess = scan_season_hint if scan_season_hint is not None else default_season

        entries = client.list_dir(scan_dir)
        for ent in entries:
            if ent.is_dir:
                continue
            ext = os.path.splitext(ent.name)[1].lower()
            if ext not in VIDEO_EXTS:
                continue

            s_hint, ep, _already_sxxeyy, _suffix = parse_episode_from_name(ent.name)
            if ep is not None:
                used_by_season.setdefault(season_guess, set()).add(ep)
                continue

            special = is_special_episode_name(ent.name) or is_special_episode_name(scan_basename)
            qishu, part_rank = parse_qishu_and_part(ent.name)
            date_key = parse_date_key(ent.name)

            if (qishu is None) and (date_key is None):
                continue

            if special:
                target_season = 0
                used_by_season.setdefault(0, set())
                sort_key = (date_key or 99999999, normalize_spaces(to_halfwidth(ent.name)).lower())
            else:
                target_season = season_guess
                used_by_season.setdefault(target_season, set())
                sort_key = (
                    qishu if qishu is not None else 9999,
                    part_rank,
                    date_key or 99999999,
                    normalize_spaces(to_halfwidth(ent.name)).lower(),
                )

            cand_by_season.setdefault(target_season, []).append((sort_key, scan_dir, ent.name, special))

    for season, cands in cand_by_season.items():
        cands.sort(key=lambda x: x[0])
        used = used_by_season.get(season, set())
        next_ep = 1
        for _key, scan_dir, fname, special in cands:
            while next_ep in used:
                next_ep += 1
            plans[(scan_dir, fname)] = (season, next_ep, special)
            used.add(next_ep)
            next_ep += 1

    return plans

def levenshtein_ratio(a: str, b: str) -> float:
    a = a.lower()
    b = b.lower()
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    n, m = len(a), len(b)
    dp = list(range(m + 1))
    for i in range(1, n + 1):
        prev = dp[0]
        dp[0] = i
        for j in range(1, m + 1):
            cur = dp[j]
            cost = 0 if a[i - 1] == b[j - 1] else 1
            dp[j] = min(dp[j] + 1, dp[j - 1] + 1, prev + cost)
            prev = cur
    dist = dp[m]
    return 1.0 - dist / max(n, m)

def ai_extract_query(ai: AIClient, folder_name: str) -> Optional[str]:
    system = "You help extract a TV series title for TMDB search. Output JSON only."
    user = (
        "Extract the most likely TV series title from this folder name. "
        "Remove quality tags, season ranges, country tags, bracketed info. "
        "Return JSON: {\"query\": string|null}.\n\n"
        f"folder_name: {folder_name}"
    )
    js = ai.chat_json(system, user)
    if not js:
        return None
    q = js.get("query")
    if isinstance(q, str):
        q = q.strip()
        return q or None
    return None

def ai_extract_queries(ai: AIClient, folder_name: str) -> List[str]:
    """Ask AI to propose multiple possible TMDB search queries (Chinese/English/romanized)."""
    system = "You help extract TV series titles for TMDB search. Output JSON only."
    user = (
        'From the folder name, propose up to 5 possible TMDB TV search queries. '
        'Remove quality tags (4K/1080p/HDR/DV/Web-DL), language tags (双语/国语/粤语/中字), '
        'collection words (合集/全集/无删减/完整版), and season ranges (1-6季). '
        'Prefer clean titles. Return JSON like: {"queries": ["title1", "title2"]}.\n\n'
        f'folder_name: {folder_name}'
    )
    js = ai.chat_json(system, user)
    if not js:
        return []
    qs = js.get("queries")
    out: List[str] = []
    if isinstance(qs, list):
        for q in qs:
            if isinstance(q, str):
                q = q.strip()
                if q and q not in out:
                    out.append(q)
    q1 = js.get("query")
    if isinstance(q1, str):
        q1 = q1.strip()
        if q1 and q1 not in out:
            out.append(q1)
    return out

def ai_choose_tmdb(
    ai: AIClient,
    folder_name: str,
    query: str,
    candidates: List[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    """Ask AI to choose the correct TMDB tv id among candidates.

    Returns tv_id (int) or None.
    """
    try:
        compact = []
        for c in (candidates or [])[:10]:
            compact.append({
                "id": c.get("id"),
                "name": c.get("name"),
                "original_name": c.get("original_name"),
                "first_air_date": c.get("first_air_date"),
                "origin_country": c.get("origin_country"),
            })
        system = "You select the best matching TMDB TV entry. Output JSON only."

        ctx = context or {}
        hints = {
            "year_hint": ctx.get("year_hint"),
            "english_title": ctx.get("english_title"),
            "max_episode": ctx.get("max_episode"),
            "episode_file_count": ctx.get("episode_file_count"),
            "sample_files": ctx.get("sample_files", [])[:6],
        }
        hints_text = json.dumps(hints, ensure_ascii=False)

        user = (
            "We are organizing a TV library. Choose the most likely TMDB TV id for the folder. "
            "Return JSON: {\"id\": number|null}. Use null if unsure.\n\n"
            "Hints may come from filenames (English title) and episode counts.\n"
            "Rules:\n"
            "- Prefer candidates whose first_air_date year matches year_hint (if provided).\n"
            "- If english_title is provided, it is often more reliable than a short Chinese name (e.g. \"怪物\").\n"
            "- If max_episode is provided (e.g. 16), prefer a series known to have that many episodes in S01.\n\n"
            f"folder_name: {folder_name}\n"
            f"tmdb_query: {query}\n"
            f"hints: {hints_text}\n"
            f"candidates: {json.dumps(compact, ensure_ascii=False)}\n"
        )
        js = ai.chat_json(system, user)
        if not js:
            return None
        picked = js.get("id")
        if isinstance(picked, (int, float)):
            return int(picked)
        if isinstance(picked, str) and picked.strip().isdigit():
            return int(picked.strip())
        return None
    except Exception:
        return None

def ai_extract_media_meta(ai: AIClient, folder_name: str, context: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Use AI to infer coarse media metadata for organize fallback.

    Returns JSON-ish dict with optional keys:
      media_type: movie | tv | anime | documentary | variety | unknown
      category: one of mapping categories when possible (电影/剧集/动漫/纪录片/综艺)
      region: broad region such as 大陆/港台/欧美/日韩/其他
      source_language: 中文/国语/粤语/英语/日语/韩语/其他
      keywords: list[str]
      confident: bool
    """
    try:
        ctx = context or {}
        payload = {
            "folder_name": folder_name,
            "year_hint": ctx.get("year_hint"),
            "english_title": ctx.get("english_title"),
            "sample_files": (ctx.get("sample_files") or [])[:8],
            "episode_file_count": ctx.get("episode_file_count"),
            "max_episode": ctx.get("max_episode"),
        }
        system = "You infer coarse media library metadata for folder organization. Output JSON only."
        user = (
            "Given a possibly messy media folder name and a few filename hints, infer coarse metadata for library organization.\n"
            "Return JSON only with this schema:\n"
            "{\n"
            "  \"media_type\": \"movie\"|\"tv\"|\"anime\"|\"documentary\"|\"variety\"|\"unknown\",\n"
            "  \"category\": \"电影\"|\"剧集\"|\"动漫\"|\"纪录片\"|\"综艺\"|null,\n"
            "  \"region\": \"大陆\"|\"港台\"|\"欧美\"|\"日韩\"|\"其他\"|null,\n"
            "  \"source_language\": \"中文\"|\"国语\"|\"粤语\"|\"英语\"|\"日语\"|\"韩语\"|\"其他\"|null,\n"
            "  \"keywords\": [string,...],\n"
            "  \"confident\": true|false\n"
            "}\n\n"
            "Rules:\n"
            "- 动漫/动画优先归到 category=动漫。\n"
            "- 中国大陆动画电影/番剧，region 优先给 大陆。\n"
            "- 香港/台湾作品可给 港台。日本/韩国作品给 日韩。欧美英语作品给 欧美。\n"
            "- If unsure, keep null and set confident=false.\n\n"
            f"input: {json.dumps(payload, ensure_ascii=False)}"
        )
        js = ai.chat_json(system, user)
        return js if isinstance(js, dict) else None
    except Exception:
        return None

def is_bad_tmdb_query(q: str) -> bool:
    """Guardrail: prevent nonsense queries from being sent to TMDB.

    Real-world failures:
      - query becomes 'S01' (season folder) -> TMDB returns unrelated shows
      - query is just a year like '2024'
    """
    if q is None:
        return True
    q2 = normalize_spaces(to_halfwidth(str(q))).strip()
    if not q2:
        return True
    if len(q2) <= 1:
        return True
    if re.fullmatch(r"(?i)s\d{1,2}", q2) or re.fullmatch(r"(?i)season\s*\d{1,2}", q2):
        return True
    if re.fullmatch(r"(?i)e\d{1,3}", q2) or re.fullmatch(r"(?i)s\d{1,2}e\d{1,3}", q2):
        return True
    if re.fullmatch(r"20\d{2}", q2) or re.fullmatch(r"20\d{2}\s*年", q2):
        return True
    if re.fullmatch(r"\d{1,4}", q2):
        return True
    return False

def resolve_series(
    tmdb: TMDBClient,
    folder_name: str,
    cache: Dict[str, Any],
    ai: Optional[AIClient],
    log: List[str],
    series_context: Optional[Dict[str, Any]] = None,
) -> Optional[SeriesMeta]:
    """Resolve a series folder name to TMDB series meta.

    Strategy
    1) Heuristic cleanup -> TMDB search (but we may try multiple queries, e.g. extracted English title)
    2) If no results, ask AI to extract possible clean titles, retry TMDB
    3) If results ambiguous, ask AI to pick best candidate (with extra context: year hint, English title, samples...)
    """
    season_hint = parse_season_from_text(folder_name)

    ctx = series_context or {}
    year_hint = ctx.get("year_hint") or extract_year_hint(folder_name)
    english_title = (ctx.get("english_title") or "").strip()

    key = clean_series_query(folder_name)
    if key in cache:
        v = cache[key]
        cached_category = v.get("category")
        cached_region = v.get("region")
        cached_confident = bool(v.get("tmdb_confident", True))
        log.append(f"[CACHE] '{folder_name}' -> {v.get('name') or folder_name} | tv_id={v.get('tv_id') or 0} | category={cached_category or '-'} | region={cached_region or '-'} | ai={bool(v.get('ai_inferred', False))} | tmdb_confident={cached_confident}")

        # Do not let old/low-quality cache entries permanently bypass the AI fallback.
        # If category/region is missing or still "其他", ask AI again and refresh the cache.
        ai_meta = None
        if ai and ((not cached_category) or cached_category == "其他" or (not cached_region) or cached_region == "其他" or (not cached_confident)):
            log.append(f"[AI] cache fallback needed '{folder_name}' | category={cached_category or '-'} | region={cached_region or '-'} | tmdb_confident={cached_confident}")
            ai_meta = ai_extract_media_meta(ai, folder_name, context=ctx) or {}
            err = ai.consume_last_error()
            if err:
                log.append(f"[AI] cache assist failed '{folder_name}' -> kind={err.get('kind')} status={err.get('status_code') or '-'} retryable={err.get('retryable')} msg={err.get('message')}")
            if ai_meta:
                ai_category = ai_meta.get("category")
                ai_region = ai_meta.get("region")
                log.append(f"[AI] cache assist '{folder_name}' -> category={ai_category or '-'} | region={ai_region or '-'} | media_type={ai_meta.get('media_type') or '-'} | source_language={ai_meta.get('source_language') or '-'}")
                if ai_category and (((not cached_category) or cached_category == "其他") or (ai_category != "其他")):
                    cached_category = ai_category
                if ai_region and (((not cached_region) or cached_region == "其他") or (ai_region != "其他")):
                    cached_region = ai_region
                v["category"] = cached_category
                v["region"] = cached_region
                v["media_type"] = ai_meta.get("media_type") or v.get("media_type")
                v["source_language"] = ai_meta.get("source_language") or v.get("source_language")
                if isinstance(ai_meta.get("keywords"), list):
                    v["keywords"] = ai_meta.get("keywords")
                v["ai_inferred"] = True

        return SeriesMeta(
            tv_id=int(v.get("tv_id") or 0),
            name=v.get("name") or folder_name,
            year=v.get("year"),
            season_hint=season_hint,
            category=cached_category,
            region=cached_region,
            media_type=v.get("media_type"),
            source_language=v.get("source_language"),
            keywords=v.get("keywords"),
            ai_inferred=bool(v.get("ai_inferred", False)),
            tmdb_confident=cached_confident,
        )

    primary_query = (key or "").strip()

    # If very messy / too short, ask AI for a query
    if (not primary_query or len(primary_query) < 2) and ai:
        q2 = ai_extract_query(ai, folder_name)
        if q2:
            primary_query = q2
            log.append(f"[AI] extracted TMDB query: {q2}  <- {folder_name}")

    def uniq_add(lst: List[str], q: str):
        q = (q or "").strip()
        if not q:
            return
        if q not in lst:
            lst.append(q)

    queries: List[str] = []
    if primary_query and not is_bad_tmdb_query(primary_query):
        uniq_add(queries, primary_query)

    # The "怪物/Beyond Evil" situation: filenames often contain a better English title than a short/ambiguous CN title.
    # Guardrail: ignore English titles that are actually season/episode tokens (e.g. "S01").
    if english_title and english_title.lower() != (primary_query or "").lower():
        if re.search(r"[A-Za-z]", english_title) and not is_bad_tmdb_query(english_title):
            uniq_add(queries, english_title)

    # Optional extra queries from context (if provided)
    for q in (ctx.get("queries") or []):
        if q and not is_bad_tmdb_query(str(q)):
            uniq_add(queries, str(q))

    if not queries:
        return None


    # Collect a pooled candidate list across multiple queries
    log.append(f"[TMDB] resolve '{folder_name}' | key='{key}' | queries={queries} | year_hint={year_hint or '-'} | english_title={english_title or '-'}")
    pooled: List[Dict[str, Any]] = []
    seen_ids: set[int] = set()
    for q in queries:
        rs = tmdb.search_tv(q)
        if rs:
            log.append(f"[TMDB] search '{q}' -> {len(rs)} results")
        for r in (rs or [])[:20]:
            rid = r.get("id")
            if rid is None:
                continue
            try:
                rid_int = int(rid)
            except Exception:
                continue
            if rid_int in seen_ids:
                continue
            seen_ids.add(rid_int)
            pooled.append(r)
        if len(pooled) >= 25:
            break

    # If still no results, ask AI for multiple candidate queries and try them
    if (not pooled) and ai:
        candidates = ai_extract_queries(ai, folder_name)
        err = ai.consume_last_error()
        if err:
            log.append(f"[AI] query_extract failed '{folder_name}' -> kind={err.get('kind')} status={err.get('status_code') or '-'} retryable={err.get('retryable')} msg={err.get('message')}")
        q2 = ai_extract_query(ai, folder_name)
        err = ai.consume_last_error()
        if err:
            log.append(f"[AI] query_extract failed '{folder_name}' -> kind={err.get('kind')} status={err.get('status_code') or '-'} retryable={err.get('retryable')} msg={err.get('message')}")
        if q2:
            candidates.append(q2)

        tried = 0
        for cand in candidates:
            cand = (cand or "").strip()
            if (not cand) or (cand in queries) or is_bad_tmdb_query(cand):
                continue
            queries.append(cand)
            tried += 1
            log.append(f"[AI] retry TMDB search with: {cand}")
            rs = tmdb.search_tv(cand)
            for r in (rs or [])[:20]:
                rid = r.get("id")
                if rid is None:
                    continue
                try:
                    rid_int = int(rid)
                except Exception:
                    continue
                if rid_int in seen_ids:
                    continue
                seen_ids.add(rid_int)
                pooled.append(r)
            if pooled:
                break
            if tried >= 5:
                break

    # If still no TMDB result, fall back to AI-only coarse metadata for organizing.
    if not pooled:
        log.append(f"[TMDB] no TMDB match for '{folder_name}' after queries={queries}")
        if ai:
            ai_meta = ai_extract_media_meta(ai, folder_name, context=ctx) or {}
            err = ai.consume_last_error()
            if err:
                log.append(f"[AI] organize fallback failed '{folder_name}' -> kind={err.get('kind')} status={err.get('status_code') or '-'} retryable={err.get('retryable')} msg={err.get('message')}")
            if ai_meta:
                log.append(f"[AI] organize fallback '{folder_name}' -> category={ai_meta.get('category') or '-'} | region={ai_meta.get('region') or '-'} | media_type={ai_meta.get('media_type') or '-'} | source_language={ai_meta.get('source_language') or '-'}")
                return SeriesMeta(
                    tv_id=0,
                    name=folder_name,
                    year=year_hint,
                    season_hint=season_hint,
                    category=ai_meta.get("category"),
                    region=ai_meta.get("region"),
                    media_type=ai_meta.get("media_type"),
                    source_language=ai_meta.get("source_language"),
                    keywords=ai_meta.get("keywords") if isinstance(ai_meta.get("keywords"), list) else None,
                    ai_inferred=True,
                    tmdb_confident=False,
                )
        return None

    def year_of(r: Dict[str, Any]) -> Optional[int]:
        d = (r.get("first_air_date") or "")
        if isinstance(d, str) and len(d) >= 4 and d[:4].isdigit():
            return int(d[:4])
        return None

    # Score candidates
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for r in pooled[:20]:
        name = r.get("name") or ""
        original = r.get("original_name") or ""
        sim = 0.0
        for q in queries:
            sim = max(sim, levenshtein_ratio(q, name), levenshtein_ratio(q, original))

        score = sim
        # Small popularity tie-breaker
        score += min(0.08, float(r.get("popularity") or 0.0) / 10000.0)

        # Year hint is important for franchises (e.g. 龙岭迷窟 2020)
        y = year_of(r)
        if year_hint and y:
            if y == int(year_hint):
                score += 0.12
            else:
                score -= 0.04

        scored.append((score, r))

    scored.sort(key=lambda x: x[0], reverse=True)
    if scored:
        top_preview = [
            {
                "id": (item.get("id")),
                "name": (item.get("name") or item.get("original_name") or ""),
                "year": year_of(item),
                "score": round(score, 4),
            }
            for score, item in scored[:5]
        ]
        log.append(f"[TMDB] candidates for '{folder_name}': {top_preview}")
    best_score, best = scored[0]
    best_name = best.get("name") or best.get("original_name") or folder_name
    log.append(f"[TMDB] picked '{folder_name}' -> {best_name} (id={best.get('id')}, score={best_score:.4f})")

    # If confidence low or top2 too close, let AI pick (with extra hints)
    if ai and (best_score < 0.72 or (len(scored) >= 2 and (scored[0][0] - scored[1][0]) < 0.03)):
        ai_ctx = dict(ctx)
        if year_hint:
            ai_ctx["year_hint"] = year_hint
        if english_title:
            ai_ctx["english_title"] = english_title
        picked = ai_choose_tmdb(ai, folder_name, primary_query or queries[0], [x[1] for x in scored], context=ai_ctx)
        if picked:
            for _, c in scored:
                if int(c.get("id")) == int(picked):
                    best = c
                    log.append(f"[AI] chose TMDB id {picked} for: {folder_name}")
                    break

    tv_id = int(best["id"])
    details = tmdb.tv_details(tv_id)
    show_name = details.get("name") or best.get("name") or primary_query or folder_name
    first_air = details.get("first_air_date") or best.get("first_air_date") or ""
    year = None
    if isinstance(first_air, str) and len(first_air) >= 4 and first_air[:4].isdigit():
        year = int(first_air[:4])

    category, region = infer_category_region_from_tmdb(details, DEFAULT_CATEGORY_REGION_MAP, title_hint=folder_name)
    log.append(f"[TMDB] metadata '{folder_name}' -> genres={[g.get('name') for g in (details.get('genres') or []) if isinstance(g, dict)]} | origins={details.get('origin_country') or details.get('production_countries') or []} | mapped={category or '-'} / {region or '-'}")
    tmdb_confident = bool(best_score >= 0.72)
    ai_meta = None
    ai_category_retry = (not category) or category == "其他"
    ai_region_retry = (not region) or region == "其他"
    if ai and (ai_category_retry or ai_region_retry or not tmdb_confident):
        ai_meta = ai_extract_media_meta(ai, folder_name, context=ctx) or {}
        err = ai.consume_last_error()
        if err:
            log.append(f"[AI] assist failed '{folder_name}' -> kind={err.get('kind')} status={err.get('status_code') or '-'} retryable={err.get('retryable')} msg={err.get('message')}")
        if ai_meta:
            ai_category = ai_meta.get("category")
            ai_region = ai_meta.get("region")
            log.append(f"[AI] assist '{folder_name}' -> category={ai_category or '-'} | region={ai_region or '-'} | media_type={ai_meta.get('media_type') or '-'} | source_language={ai_meta.get('source_language') or '-'}")
            if ai_category and ((not category) or category == "其他") and ai_category != "其他":
                category = ai_category
            if ai_region and ((not region) or region == "其他") and ai_region != "其他":
                region = ai_region
            if not category and ai_category:
                category = ai_category
            if not region and ai_region:
                region = ai_region

    cache[key] = {
        "tv_id": tv_id,
        "name": show_name,
        "year": year,
        "category": category,
        "region": region,
        "media_type": (ai_meta or {}).get("media_type"),
        "source_language": (ai_meta or {}).get("source_language"),
        "keywords": (ai_meta or {}).get("keywords") if isinstance((ai_meta or {}).get("keywords"), list) else None,
        "ai_inferred": bool(ai_meta),
        "tmdb_confident": tmdb_confident,
    }
    log.append(f"[TMDB] final '{folder_name}' -> {show_name} ({year or '-'}) | category={category or '-'} | region={region or '-'} | ai={bool(ai_meta)} | tmdb_confident={tmdb_confident}")
    return SeriesMeta(
        tv_id=tv_id,
        name=show_name,
        year=year,
        season_hint=season_hint,
        category=category,
        region=region,
        media_type=(ai_meta or {}).get("media_type"),
        source_language=(ai_meta or {}).get("source_language"),
        keywords=(ai_meta or {}).get("keywords") if isinstance((ai_meta or {}).get("keywords"), list) else None,
        ai_inferred=bool(ai_meta),
        tmdb_confident=tmdb_confident,
    )

__all__ = [name for name in globals() if not name.startswith("__")]
