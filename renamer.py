#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AList -> Emby TV folder organizer & renamer (safe, slow, TMDB-powered) + optional AI assistance

✅ You asked to "use AI when it's hard". This version keeps your strict rules:
1) If ANY video filename already contains SxxEyy (case-insensitive), the video file is NEVER renamed.
   - But the file can still be moved into the correct season folder.
2) Series folder is normalized to: "剧名 (Year)" (Year = first_air_date year from TMDB).
3) Season folder normalized to: "S01", "S02", ...
4) Files without SxxEyy (e.g. "01.mp4", "E01.mkv") are renamed to:
   "剧名 - S01E01 - (可选后缀).ext"
5) Subtitle sidecars (.srt/.ass/.ssa/.vtt/.sub/.idx/.sup) are renamed to match the final video stem.
6) "散落的季"合并：在剧根目录/质量文件夹(如“4K高码 DV HDR”)里发现 S04E.. 等，会自动创建/使用 S04 文件夹并移动进去。
7) 支持把类似 “S1-S3” 这种“季打包目录”里的 S1/S2/S3 迁移到剧根目录，并改名为 S01/S02/S03。

🧠 AI assistance (optional):
- When TMDB search has no results or low confidence, AI can:
  (a) extract a better search query from the messy folder name
  (b) choose the best TMDB candidate among top results
- When a video's season is missing (e.g. E01.mp4) but the folder name implies “第四季/S4”,
  AI can help infer the season (only if the deterministic hints fail).

OpenAI-compatible API is supported (any "AI gateway" that implements /v1/chat/completions).

Refs:
- AList fs endpoints: https://alistgo.com/zh/guide/api/fs.html
- Emby TV naming: https://emby.media/support/articles/TV-Naming.html
- OpenAI Chat Completions: https://platform.openai.com/docs/api-reference/chat
- OpenAI Structured Outputs / JSON: https://platform.openai.com/docs/guides/structured-outputs

Author: generated for user @kingkang527
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import sys
import time
import threading
import logging

logger = logging.getLogger("embyrename")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-5s | %(message)s")

from datetime import datetime, date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlencode

try:
    import requests
except ImportError:
    print("Missing dependency: requests. Install with: pip install requests", file=sys.stderr)
    raise


from logui import LogHub, LiveLog, start_log_server
from runtime_config import RuntimeConfigStore, apply_runtime_config, CURRENT_RUNTIME_CONFIG

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".m4v", ".ts", ".m2ts", ".webm"}
SUB_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx", ".sup"}
DEFAULT_CATEGORY_REGION_MAP = {
    "电影": ["大陆", "港台", "欧美", "日韩", "其他"],
    "剧集": ["大陆", "港台", "欧美", "日韩", "其他"],
    "动漫": ["大陆", "港台", "欧美", "日韩", "其他"],
}

SXXEYY_RE = re.compile(r"(?i)S(\d{1,2})\s*E(\d{1,3})")
EYY_RE = re.compile(r"(?i)\bE(\d{1,3})\b")


def norm_path(p: str) -> str:
    if not p:
        return "/"
    p = p.replace("\\", "/")
    if not p.startswith("/"):
        p = "/" + p
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    return p


def join_path(dir_path: str, name: str) -> str:
    dir_path = norm_path(dir_path)
    if dir_path == "/":
        return "/" + name
    return dir_path + "/" + name


def split_path(p: str) -> Tuple[str, str]:
    p = norm_path(p)
    if p == "/":
        return ("/", "")
    parent, _, base = p.rpartition("/")
    return (parent if parent else "/", base)


def now_ts() -> str:
    """Timestamp string for logs/state/undo.

    Keep it simple and locale-safe.
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_config_dir() -> str:
    """Resolve config directory from the current code directory only."""
    return os.path.abspath(os.path.dirname(__file__))


def normalize_spaces(s: str) -> str:
    """Collapse all whitespace (including weird unicode spaces) into single spaces."""
    if s is None:
        return ""
    # NBSP and other odd spaces
    s = str(s).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE)
    return s.strip()


def to_halfwidth(s: str) -> str:
    """Convert fullwidth unicode chars (digits/letters/punct) to halfwidth.

    This makes parsing robust for names like '４Ｋ', '２１６０Ｐ', 'Ｓ０１'.
    """
    if s is None:
        return ""
    out = []
    for ch in str(s):
        code = ord(ch)
        if code == 0x3000:  # fullwidth space
            out.append(" ")
        elif 0xFF01 <= code <= 0xFF5E:  # fullwidth ASCII
            out.append(chr(code - 0xFEE0))
        else:
            out.append(ch)
    return "".join(out)


# ---------------------------------------------------------------------------
# Heuristic config (safe defaults)
# ---------------------------------------------------------------------------

# Folder names we should ignore when trying to detect nested/collection show folders.
# (Also used by some clean-up routines.)
MISC_DIR_NAMES = {
    "@eadir", "__macosx", ".ds_store",
    "sample", "samples", "screens", "screen", "screenshots",
    "extras", "extra", "bonus", "bts",
    "poster", "posters", "fanart", "thumb", "thumbs", "artwork",
    "cd1", "cd2",
    "subs", "sub", "subtitle", "subtitles", "字幕", "字幕组",
}

# Directory names that commonly contain subtitle files (we will *move* subtitles out; not delete).
SUBTITLE_DIR_NAMES = {"subs", "sub", "subtitle", "subtitles", "字幕", "字幕组", "subtitles&subs"}

# File extensions we treat as subtitles/sidecars.
SUBTITLE_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx"}

# File extensions that are almost always advertisements / useless for Emby scraping.
# User requirement: DO NOT delete .txt.
AD_DELETE_EXTS = {
    ".url", ".lnk", ".html", ".htm",
    ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
}

# Common ad markers (if these appear in file/folder names, we consider them junky).
JUNK_MARKERS = [
    "防走丢", "更多资源", "公众号", "关注", "扫码", "加群", "群号", "最新地址",
    "备用网址", "网址", "www.", "http://", "https://", "telegram", "t.me", "qq群", "qq群号",
]

def bool_env(name: str, default: bool = False) -> bool:
    cfg_key = str(name or '').strip().lower()
    if cfg_key in CURRENT_RUNTIME_CONFIG:
        return bool(CURRENT_RUNTIME_CONFIG.get(cfg_key, default))
    return default






QUALITY_TOKEN_MAP = {
    # normalize common quality tokens (preserve meaning, adjust casing)
    "4k": "4K",
    "uhd": "UHD",
    "hdr": "HDR",
    "hdr10": "HDR10",
    "hdr10+": "HDR10+",
    "dv": "DV",
    "dovi": "DV",
    "dolby": "Dolby",
    "dolbyvision": "DolbyVision",
    "dolby vision": "DolbyVision",
}

CN_QUALITY_MAP = {
    "杜比视界": "DV",
    "杜比": "Dolby",
    "视界": "DV",
    "高码": "HiBitrate",
}


def normalize_quality_tail(text: str) -> str:
    """Normalize quality tail tokens but keep everything.

    - 4k -> 4K
    - hdr -> HDR
    - dolby/dolbyvision -> Dolby/DolbyVision
    - keep 2160p/1080p etc as-is (lowercase p)
    """
    if not text:
        return text
    t = text
    # normalize Chinese quality hints
    for k, v in CN_QUALITY_MAP.items():
        t = t.replace(k, v)
    # normalize resolution like 2160P/1080P
    t = re.sub(r"(?i)\b(\d{3,4})p\b", lambda m: f"{m.group(1)}p", t)
    t = re.sub(r"(?i)\b(\d{3,4})P\b", lambda m: f"{m.group(1)}p", t)
    # normalize 4k token
    t = re.sub(r"(?i)(?<![A-Za-z0-9])4k(?![A-Za-z0-9])", "4K", t)
    # normalize HDR/DV/Dolby tokens (case-insensitive)
    def _norm_token(m):
        raw = m.group(0)
        key = raw.lower()
        return QUALITY_TOKEN_MAP.get(key, raw)
    # handle multi-word 'dolby vision'
    t = re.sub(r"(?i)dolby\s+vision", "DolbyVision", t)
    t = re.sub(r"(?i)\b(dolbyvision|dolby|hdr10\+|hdr10|hdr|dovi|dv|uhd)\b", _norm_token, t)
    # collapse spaces
    t = re.sub(r"\s+", " ", t).strip()
    return t
def safe_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', " ", name).strip()

def unique_name_in_parent(client: 'AlistClient', parent: str, desired: str) -> str:
    """Resolve name conflict within a directory.

    on_conflict:
      - suffix (default): append " (1)", " (2)" ...
      - skip: return empty string to indicate skip
    """
    mode = str(CURRENT_RUNTIME_CONFIG.get("on_conflict", "suffix") or "suffix").strip().lower()
    parent = norm_path(parent)
    desired = safe_filename(desired)
    try:
        entries = client.list_dir(parent, refresh=False)
        existing = {e.name for e in entries}
    except Exception:
        existing = set()
    if desired not in existing:
        return desired
    if mode == "skip":
        return ""
    stem, ext = os.path.splitext(desired)
    for i in range(1, 200):
        cand = f"{stem} ({i}){ext}"
        if cand not in existing:
            return cand
    return ""



def clean_series_query(folder_name: str) -> str:
    """Heuristic cleanup; AI may refine further if enabled.

    Goal: turn messy folder names like
      - 鹿鼎记 双语4K
      - 浴血黑帮1-6季 无删减 合集
      - (US) Silo.S02 2160p DV HDR
    into a search-friendly title.

    We *keep* meaningful title punctuation (e.g. ：) but strip common tags.
    """
    s = normalize_spaces(to_halfwidth(folder_name))

    # remove leading release group / bracket tags
    s = re.sub(r"^\[[^\]]{1,60}\]\s*", "", s)

    # remove obvious season bundles, disk notes
    s = re.sub(r"\s*(?:全\d+季|\d+\s*Season|S\d{1,2}-S\d{1,2}|\d{1,2}-\d{1,2}季|\d{1,2}季合集|合集)\s*", " ", s, flags=re.I)
    # remove patterns like " 4 附带1-3" (season marker used for packaging)
    s = re.sub(r"\s+\d{1,2}\s*(?:附带|含|带)\s*\d{1,2}\s*[-~—–]\s*\d{1,2}.*$", " ", s)


    # trailing numeric range like "地球脉动1-3" / "1~3" (no explicit "季")
    s = re.sub(r"(?<=\D)\d{1,2}\s*[-~—–]\s*\d{1,2}\s*$", " ", s)

    # remove quality/resolution/audio tags (CN + EN)
    tags = [
        r"\b(?:2160p|1080p|720p|480p|4k|8k)\b",
        r"\b(?:web[-_. ]?dl|webrip|bluray|bdrip|hdrip|remux|x26[45]|hevc|avc|h\.264|h\.265)\b",
        r"\b(?:dv|dolby\s*vision|hdr10\+?|hdr)\b",
        r"\b(?:aac|ac-?3|ddp?|truehd|dts(?:-?hd)?)\b",
        r"\b(?:atvp|nf|amzn|hmax|dsnp|hulu)\b",
        r"\b(?:10bit|8bit)\b",
        r"\b(?:proper|repack|extended|uncut)\b",
        r"(?:中英双字|中英字幕|中字|双字|双语|国语|粤语|英语|日语|韩语|无删减|未删减|删减|精修|修复|高码|高码率|收藏版|剧场版|OVA|SP|特典|花絮|完整版)",
    ]
    for t in tags:
        s = re.sub(t, " ", s, flags=re.I)

    # strip season suffix like " 第一季"/" 第四季" (title-only for search)
    s = re.sub(r"\s+第[一二三四五六七八九十\d]{1,3}季\b", "", s)

    # strip years at end like " 2025"/"(2025)"/"[2025]" while keeping them if embedded in title
    s = re.sub(r"\s*[\(\[（]?\s*(19\d{2}|20\d{2})\s*[\)\]）]?\s*$", "", s)

    # collapse spaces
    s = normalize_spaces(s)

    # final trim of punctuation-only leftovers
    s = s.strip("-_. ")
    return s


CN_NUM = {"零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}


def chinese_to_int(s: str) -> Optional[int]:
    s = s.strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    # handle 1..99 in common forms: 十, 十一..十九, 二十..九十九
    if s == "十":
        return 10
    if len(s) == 2 and s[0] == "十" and s[1] in CN_NUM:
        return 10 + CN_NUM[s[1]]
    if len(s) == 2 and s[1] == "十" and s[0] in CN_NUM:
        return CN_NUM[s[0]] * 10
    if len(s) == 3 and s[1] == "十" and s[0] in CN_NUM and s[2] in CN_NUM:
        return CN_NUM[s[0]] * 10 + CN_NUM[s[2]]
    # fallback: sum digits
    total = 0
    for ch in s:
        if ch not in CN_NUM:
            return None
        total = total * 10 + CN_NUM[ch]
    return total


def parse_season_from_text(text: str) -> Optional[int]:
    """Parse season from strings like S4/S04, Season 4, 第四季/第4季, 4季.

    Also handles cases where 'S1' is adjacent to CJK characters (e.g. '安多S1'),
    while avoiding matching patterns like 'S01E02'.
    """
    t = to_halfwidth(text or "").strip()
    if not t:
        return None
    # Guard: season-range container folders like "S1-S4" / "S01-S04" / "1-4季"
    # These are NOT a single season and should not be parsed as season=1.
    if re.search(r"(?i)\bS\d{1,2}\s*[-~—–]\s*S?\d{1,2}\b", t):
        return None
    if re.search(r"(?:第\s*)?\d{1,2}\s*[-~—–]\s*\d{1,2}\s*季", t):
        return None


    # Standalone season marker, e.g. 'S1' / 'S01' / '安多S1' (but NOT 'S01E02')
    m = re.search(r"(?i)(?:^|[^A-Za-z0-9])S(\d{1,2})(?:$|[^A-Za-z0-9])", t)
    if m:
        return int(m.group(1))

    m = re.search(r"(?i)\bSeason\s*(\d{1,2})\b", t)
    if m:
        return int(m.group(1))

    m = re.search(r"第\s*([一二三四五六七八九十\d]+)\s*季", t)
    if m:
        return chinese_to_int(m.group(1))

    m = re.search(r"\b(\d{1,2})\s*季\b", t)
    if m:
        return int(m.group(1))

    # also accept "第X部" / "X部" as season markers (common in anime franchises)
    m = re.search(r"第\s*([一二三四五六七八九十\d]+)\s*部", t)
    if m:
        return chinese_to_int(m.group(1))

    m = re.search(r"\b(\d{1,2})\s*部\b", t)
    if m:
        return int(m.group(1))

    # e.g. '我爱你 4 附带1-3' -> season=4 (root files belong to season 4)
    m = re.search(r"(?:^|\D)(\d{1,2})\s*(?:附带|含|带)\s*\d{1,2}\s*[-~—–]\s*\d{1,2}", t)
    if m:
        return int(m.group(1))

    return None



_YEAR_HINT_RE = re.compile(r"(19\d{2}|20\d{2})")


def extract_year_hint(text: str) -> Optional[int]:
    """Extract a plausible year (1900-2099) from text."""
    m = _YEAR_HINT_RE.search(text or "")
    if not m:
        return None
    try:
        y = int(m.group(1))
    except Exception:
        return None
    if 1900 <= y <= 2099:
        return y
    return None


def normalize_title_for_compare(text: str) -> str:
    """Normalization for loose comparisons (used for heuristics only)."""
    t = (text or "").lower()
    t = re.sub(r"[\s._\-]+", "", t)
    t = re.sub(r"[\[\]【】()（）{}<>《》]", "", t)
    return t


CATEGORY_CONTAINER_NAMES = {
    "国创", "国产动漫", "欧美动漫", "日韩动漫", "日漫", "国漫", "动漫", "动漫剧集",
    "电视剧", "剧集", "大陆", "港台", "欧美", "日韩", "其他",
    "大陆剧", "港台剧", "欧美剧", "日韩剧", "其他剧集",
    "纪录片", "综艺", "电影", "短剧",
}


def is_season_dir(name: str) -> bool:
    return parse_season_from_text(name) is not None


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


def season_folder_name(season: int, fmt: str = "S{season:02d}") -> str:
    """Build season folder name.

    Users may override via SEASON_FORMAT. To avoid crashing on a bad format string
    (e.g. stray '}' in .env), we fall back to the safe default.
    """
    # Emby recognizes Season 0 as "Specials"
    if season == 0:
        return "Specials"

    fmt = (fmt or "S{season:02d}").strip()
    try:
        return fmt.format(season=season)
    except Exception:
        # support printf-style like "S%02d" (optional)
        try:
            if "%" in fmt:
                return fmt % season
        except Exception:
            pass
        return f"S{season:02d}"


# ---------------- Episode parsing helpers ----------------

_EP_NUM_RE = re.compile(r"(?i)\b(?:EP|E)(\d{1,3})\b")
_1X02_RE = re.compile(r"(?i)\b(\d{1,2})\s*[xX]\s*(\d{1,3})\b")
_CN_EP_RE = re.compile(r"第\s*([一二三四五六七八九十\d]{1,4})\s*(?:集|话|回)")


def _quality_tokens(text: str) -> List[str]:
    """Extract a compact quality/release tail from a name.

    Best-effort: should never raise.
    """
    try:
        t = (text or "").lower()
    except Exception:
        return []

    pats = [
        r"\b4k\b", r"\b2160p\b", r"\b1080p\b", r"\b720p\b",
        r"\bhdr10\+?\b", r"\bhdr\b", r"\bdv\b", r"dolby\s*vision",
        r"web[- ]?dl", r"webrip", r"bluray", r"bdrip", r"remux",
        r"hevc", r"x265", r"h265", r"x264", r"h264",
        r"truehd", r"dts[- ]?hd", r"\bdts\b", r"\baac\b", r"\batmos\b",
        r"\bnf\b", r"\bamzn\b", r"\bhmax\b",
        r"\b中字\b", r"\b双语\b", r"\b国配\b", r"\b国语\b", r"\b粤语\b", r"\b中英\b",
    ]

    out: List[str] = []
    for p in pats:
        try:
            m = re.search(p, t)
        except Exception:
            m = None
        if not m:
            continue
        tok = m.group(0)
        tok = re.sub(r"\s+", "", tok)
        if tok in ("web-dl", "weBDL", "weBdl"):
            tok = "WEB-DL"
        if tok.lower() == "webrip":
            tok = "WEBRIP"
        if tok.lower() == "bluray":
            tok = "BluRay"
        if tok not in out:
            out.append(tok)
    return out


def parse_episode_from_name(name: str) -> Tuple[Optional[int], Optional[int], bool, str]:
    """Parse (season, episode) from a filename or folder name.

    Returns: (season, episode, already_has_sxxeyy, suffix)
      - season can be None if unknown
      - episode can be None if cannot infer
      - already_has_sxxeyy=True when filename already contains explicit SxxEyy/1x02
      - suffix is a best-effort quality tail (4K/HDR/WEB-DL/中字...)
    """
    raw = (name or "").strip()
    if not raw:
        return None, None, False, ""

    base = os.path.basename(raw)
    stem, _ext = os.path.splitext(base)

    # remove leading release tags like [xxx] or 【xxx】
    stem2 = re.sub(r"^\[[^\]]+\]\s*", "", stem).strip()
    stem2 = re.sub(r"^[【\[][^】\]]+[】\]]\s*", "", stem2).strip()

    stem2 = normalize_spaces(to_halfwidth(stem2))
    # If this looks like a multi-season container folder (e.g. "1-4季" / "S1-S3"),
    # do NOT treat embedded numbers as episode numbers.
    is_season_range_container = bool(re.search(r"(?:第\s*)?\d{1,2}\s*[-~—–]\s*\d{1,2}\s*季", stem2)) or \
        bool(re.search(r"(?i)\bS\d{1,2}\s*[-~—–]\s*S?\d{1,2}\b", stem2))

    # season hint embedded in name
    season_hint = parse_season_from_text(stem2)

    # SxxEyy
    m = SXXEYY_RE.search(stem2)
    if m:
        s = int(m.group(1))
        e = int(m.group(2))
        rest = stem2[m.end():].strip(" ._-")
        suffix = " ".join(_quality_tokens(stem2 + " " + rest))
        return s, e, True, suffix

    # 1x02
    m = _1X02_RE.search(stem2)
    if m:
        s = int(m.group(1))
        e = int(m.group(2))
        rest = stem2[m.end():].strip(" ._-")
        suffix = " ".join(_quality_tokens(stem2 + " " + rest))
        return s, e, True, suffix

    # E02 / EP02
    m = _EP_NUM_RE.search(stem2)
    if m:
        e = int(m.group(1))
        return season_hint, e, False, " ".join(_quality_tokens(stem2))

    # 第xx集/话/回
    m = _CN_EP_RE.search(stem2)
    if m:
        e = chinese_to_int(m.group(1))
        return season_hint, e, False, " ".join(_quality_tokens(stem2))

            # leading episode number like "01" or "002" (avoid year / large numbers)
    if not is_season_range_container:
        m = re.match(r"^\s*(\d{1,3})(?!\d)", stem2)
        if m:
            e = int(m.group(1))
            if 1 <= e <= 200:
                return season_hint, e, False, " ".join(_quality_tokens(stem2))

    # standalone episode number token somewhere, e.g. "暗河传 28 4K"
    if not is_season_range_container:
        nums = [int(x) for x in re.findall(r"(?:^|[\s._\-])0*(\d{1,3})(?=$|[\s._\-])", stem2)]
        nums = [n for n in nums if 1 <= n <= 200]
        if nums:
            # Prefer the last standalone number: titles like "创世纪 2 天地有情 38"
            # contain a series-part number before the real episode number.
            return season_hint, nums[-1], False, " ".join(_quality_tokens(stem2))

    # glued trailing episode number, e.g. "创世纪2天地有情38"
    if not is_season_range_container:
        m = re.search(r"(?<!\d)(\d{1,3})$", stem2)
        if m:
            e = int(m.group(1))
            if 1 <= e <= 200:
                prefix = stem2[:m.start(1)]
                # Avoid treating plain year/resolution tails as episodes.
                if not re.search(r"(?:19|20)\d{2}\s*$", prefix) and not re.search(r"(?:2160|1080|720|480)[pi]?\s*$", prefix, re.I):
                    return season_hint, e, False, " ".join(_quality_tokens(stem2))




# ---------------------------------------------------------------------------
# Variety show / Specials helpers
# ---------------------------------------------------------------------------

_SPECIAL_MARKERS = [
    "抢先看",
    "预告",
    "先导",
    "花絮",
    "幕后",
    "特辑",
    "特别篇",
    "番外",
    "彩蛋",
    "sp",
    "special",
    "pv",
    "cm",
]


def is_special_episode_name(name: str) -> bool:
    n = to_halfwidth(name or "").lower()
    for m in _SPECIAL_MARKERS:
        if m.lower() in n:
            return True
    return False


_DATE8_RE = re.compile(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")
_DATE_SEP_RE = re.compile(r"(?<!\d)(20\d{2})[.\-_](\d{1,2})[.\-_](\d{1,2})(?!\d)")


def parse_date_key(text: str) -> Optional[int]:
    """Return yyyymmdd as int if a plausible date exists in text."""
    t = to_halfwidth(text or "")
    m = _DATE8_RE.search(t)
    if not m:
        m = _DATE_SEP_RE.search(t)
    if not m:
        return None
    y = int(m.group(1))
    mo = int(m.group(2))
    d = int(m.group(3))
    try:
        date(y, mo, d)
    except Exception:
        return None
    return y * 10000 + mo * 100 + d


_PART_ORDER = {
    "上": 1,
    "上集": 1,
    "上期": 1,
    "前": 1,
    "中": 2,
    "中集": 2,
    "中期": 2,
    "下": 3,
    "下集": 3,
    "下期": 3,
    "后": 2,
}


_QISHU_RE = re.compile(r"第\s*([一二三四五六七八九十\d]{1,4})\s*期")


def parse_qishu_and_part(text: str) -> Tuple[Optional[int], int]:
    """Parse '第10期上/下' -> (10, part_rank)."""
    t = to_halfwidth(text or "")
    m = _QISHU_RE.search(t)
    if not m:
        return None, 0
    qishu = chinese_to_int(m.group(1))
    part_rank = 0
    tail = t[m.end() : m.end() + 8]  # small window after "第X期"
    for k, v in _PART_ORDER.items():
        if k in tail:
            part_rank = v
            break
    return qishu, part_rank


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


def needs_series_prefix_for_sxxeyy(filename: str, series_name: str) -> bool:
    """True if filename is bare SxxEyy/1x02 without series title."""
    if not filename or not series_name:
        return False
    stem = os.path.splitext(os.path.basename(filename))[0]
    stem2 = re.sub(r"^\[[^\]]+\]\s*", "", stem).strip()
    low = stem2.lower()
    if series_name.lower() in low:
        return False
    if re.match(r"(?i)^S\d{1,2}\s*E\d{1,3}\b", stem2):
        return True
    if re.match(r"(?i)^\d{1,2}\s*[xX]\s*\d{1,3}\b", stem2):
        return True
    return False


COMMON_LANGS = {
    "en","zh","ja","ko","fr","de","es","it","ru","pt","ar","nl","sv","no","da","fi","pl","cs","hu","tr",
    "th","vi","id","ms","he","el","uk","ro","bg","hr","sr","sk","sl","et","lv","lt","fa","ur",
}

def _normalize_lang_token(tok: str) -> Optional[str]:
    """Normalize a candidate language token to Emby/Plex-friendly tag."""
    if not tok:
        return None
    t = tok.strip().lower()
    t = t.replace("_", "-")

    mapping = {
        "en": "en", "eng": "en",
        "chs": "chs", "sc": "chs", "zh-cn": "chs", "zh-hans": "chs", "zhcn": "chs", "gb": "chs", "简体": "chs",
        "cht": "cht", "tc": "cht", "zh-tw": "cht", "zh-hant": "cht", "zhtw": "cht", "big5": "cht", "繁体": "cht",
        "zh": "zh", "chi": "zh", "zho": "zh",
        "ja": "ja", "jpn": "ja", "jp": "ja",
        "ko": "ko", "kor": "ko", "kr": "ko",
        "es": "es", "spa": "es",
        "fr": "fr", "fra": "fr",
        "de": "de", "deu": "de",
        "it": "it", "ita": "it",
        "ru": "ru", "rus": "ru",
        "pt": "pt", "por": "pt",
        "pt-br": "pt-br", "ptbr": "pt-br",
        "ar": "ar", "ara": "ar",
    }
    if t in mapping:
        return mapping[t]

    # tokens like "zh-hans", "zh-hant"
    if t.startswith("zh-"):
        if "hans" in t or t.endswith("-cn"):
            return "chs"
        if "hant" in t or t.endswith("-tw"):
            return "cht"
        return "zh"

    # tokens like "en-us" -> "en"
    if re.fullmatch(r"[a-z]{2}-[a-z]{2}", t):
        base = t.split("-")[0]
        if base in COMMON_LANGS:
            return base

    if t in COMMON_LANGS:
        return t

    return None


def _extract_subtitle_lang_and_flags(filename: str) -> Tuple[Optional[str], List[str]]:
    """Extract language + common flags (forced/sdh/hi) from a subtitle filename."""
    base = os.path.splitext(os.path.basename(filename))[0]
    low = base.lower()

    flags: List[str] = []
    if "forced" in low or "forc" in low:
        flags.append("forced")

    # hearing-impaired / SDH / CC
    tokens_for_flags = re.split(r"[\W_]+", low)
    if "sdh" in tokens_for_flags or "cc" in tokens_for_flags or "hi" in tokens_for_flags or "hearing" in low:
        flags.append("sdh")

    # Special Chinese hints
    if any(x in base for x in ["简体", "简中", "chs", "sc"]):
        return "chs", flags
    if any(x in base for x in ["繁体", "繁中", "cht", "tc"]):
        return "cht", flags
    if "中英" in base or "双语" in base:
        # one file contains both; keep it as Chinese to avoid collisions
        return "chs", flags

    # common combined tokens (avoid being split by tokenization)
    low_norm = low.replace("_", "-")
    if re.search(r"\bzh-hant\b", low_norm) or re.search(r"\bzh-tw\b", low_norm):
        return "cht", flags
    if re.search(r"\bzh-hans\b", low_norm) or re.search(r"\bzh-cn\b", low_norm):
        return "chs", flags
    if re.search(r"\bpt-br\b", low_norm):
        return "pt-br", flags

    # split tokens by separators
    tokens = re.split(r"[\s._\-\[\](){}]+", base)
    lang: Optional[str] = None
    for tok in tokens:
        lt = _normalize_lang_token(tok)
        if lt:
            lang = lt
            break

    return lang, flags



def build_new_sidecar_name(video_stem: str, old_sidecar_name: str, season: int, episode: int) -> str:
    """Build new sidecar/subtitle name following Emby/Plex style.

    User preference: E01.en.srt style (i.e. lang comes *before* extension)
    Examples:
      - Show (2020) - S01E01.mp4
      - Show (2020) - S01E01.en.srt
      - Show (2020) - S01E01.chs.forced.ass
    """
    _base, ext = os.path.splitext(old_sidecar_name)
    ext = ext.lower()

    # Only apply language/flags to real subtitle formats.
    if ext not in SUB_EXTS and ext not in SUBTITLE_EXTS:
        return f"{video_stem}{ext}"

    lang, flags = _extract_subtitle_lang_and_flags(old_sidecar_name)

    parts = [video_stem]
    if lang:
        parts.append(lang)
    # keep stable order
    for f in ["forced", "sdh"]:
        if f in flags:
            parts.append(f)

    # join with dots
    new_base = ".".join(parts)
    return f"{new_base}{ext}"


@dataclasses.dataclass
class DirEntry:
    name: str
    is_dir: bool


class RateLimiter:
    def __init__(self, min_interval_sec: float):
        self.min_interval_sec = max(0.0, float(min_interval_sec))
        self._last = 0.0

    def wait(self):
        if self.min_interval_sec <= 0:
            return
        now = time.time()
        delta = now - self._last
        if delta < self.min_interval_sec:
            time.sleep(self.min_interval_sec - delta)
        self._last = time.time()


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
                out.append(DirEntry(name=it.get("name", ""), is_dir=bool(it.get("is_dir"))))
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


class TMDBClient:
    def __init__(self, api_key: str, language: str = "zh-CN", sleep: float = 0.3, timeout: float = 20.0):
        self.api_key = api_key
        self.language = language
        self.timeout = timeout
        self.rl_read = RateLimiter(float(CURRENT_RUNTIME_CONFIG.get("alist_sleep_read", sleep)))
        self.rl_write = RateLimiter(float(CURRENT_RUNTIME_CONFIG.get("alist_sleep_write", max(1.2, float(sleep)))))
        self.retries = int(CURRENT_RUNTIME_CONFIG.get("alist_retries", 5))
        self.retry_base = float(CURRENT_RUNTIME_CONFIG.get("alist_retry_base", 0.8))
        self.retry_max = float(CURRENT_RUNTIME_CONFIG.get("alist_retry_max", 10.0))
        # TMDB 在部分网络环境可能无法直连。
        # 你现在用的代理形态通常分两类：
        #   1) 官方：  https://api.themoviedb.org/3/...
        #   2) 代理：  https://<proxy>/get/...   （把 /get/ 映射到官方 /3/）
        #
        # 约定（按你的说明）：
        #   - https://www.example.com/get/  <=>  https://api.themoviedb.org/3/
        #   - https://www.example.com/img/  <=>  https://image.tmdb.org/
        #
        # 因此：
        #   - 你填 api.themoviedb.org（或 themoviedb.org）时，自动补 /3
        #   - 你填其它域名（如 tmdb.melonhu.cn）时，自动补 /get
        #   - 若你已经显式写了 /get 或 /3，就保持不变
        base = str(CURRENT_RUNTIME_CONFIG.get("tmdb_api_base", "") or "").strip()
        if base:
            base = base.rstrip("/")
            if base.endswith("/get") or base.endswith("/3"):
                self.base = base
            else:
                low = base.lower()
                # 官方域名：自动补 /3
                if "themoviedb.org" in low:
                    self.base = base + "/3"
                else:
                    # 代理域名：自动补 /get
                    self.base = base + "/get"
        else:
            self.base = "https://api.themoviedb.org/3"

    def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self.rl_read.wait()
        url = self.base + path
        params = dict(params)
        params["api_key"] = self.api_key
        r = requests.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def search_tv(self, query: str) -> List[Dict[str, Any]]:
        return (self.get("/search/tv", {"query": query, "language": self.language}).get("results") or [])

    def tv_details(self, tv_id: int) -> Dict[str, Any]:
        return self.get(f"/tv/{tv_id}", {"language": self.language})


class AIClient:
    """OpenAI-compatible /v1/chat/completions client (optional)."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        sleep: float = 1.0,
        timeout: float = 60.0,
        verify_tls: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.verify_tls = verify_tls
        self.rl_read = RateLimiter(float(CURRENT_RUNTIME_CONFIG.get("alist_sleep_read", sleep)))
        self.rl_write = RateLimiter(float(CURRENT_RUNTIME_CONFIG.get("alist_sleep_write", max(1.2, float(sleep)))))
        self.retries = int(CURRENT_RUNTIME_CONFIG.get("alist_retries", 5))
        self.retry_base = float(CURRENT_RUNTIME_CONFIG.get("alist_retry_base", 0.8))
        self.retry_max = float(CURRENT_RUNTIME_CONFIG.get("alist_retry_max", 10.0))
        self.last_error: Optional[Dict[str, Any]] = None

    def _set_last_error(self, kind: str, message: str, status_code: Optional[int] = None, retryable: bool = False):
        self.last_error = {
            "kind": kind,
            "message": message,
            "status_code": status_code,
            "retryable": bool(retryable),
            "at": now_ts(),
        }

    def consume_last_error(self) -> Optional[Dict[str, Any]]:
        err = self.last_error
        self.last_error = None
        return err

    def _parse_json_from_text(self, text: str) -> Optional[dict]:
        text = text.strip()
        # best-effort: extract first {...}
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            chunk = text[start : end + 1]
            try:
                return json.loads(chunk)
            except Exception:
                return None
        return None

    def chat_json(self, system: str, user: str, json_mode: bool = True, max_tokens: int = 400) -> Optional[dict]:
        """Return a JSON object (or None)."""
        self.last_error = None
        self.rl_read.wait()
        url = self.base_url + ("/chat/completions" if self.base_url.rstrip("/").endswith("/v1") else "/v1/chat/completions")
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0,
            "max_tokens": max_tokens,
        }
        if json_mode:
            payload["response_format"] = {"type": "json_object"}

        attempts = max(1, min(int(self.retries or 1), 3))
        for attempt in range(1, attempts + 1):
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=self.timeout, verify=self.verify_tls)
            except requests.exceptions.Timeout as e:
                retryable = attempt < attempts
                msg = f"AI timeout: {e}"
                self._set_last_error("timeout", msg, retryable=retryable)
                logger.warning("[AI] %s (attempt %s/%s)", msg, attempt, attempts)
                if retryable:
                    time.sleep(min(self.retry_max, self.retry_base * (2 ** (attempt - 1))))
                    continue
                return None
            except requests.exceptions.ConnectionError as e:
                retryable = attempt < attempts
                msg = f"AI connection error: {e}"
                self._set_last_error("connection", msg, retryable=retryable)
                logger.warning("[AI] %s (attempt %s/%s)", msg, attempt, attempts)
                if retryable:
                    time.sleep(min(self.retry_max, self.retry_base * (2 ** (attempt - 1))))
                    continue
                return None
            except requests.exceptions.RequestException as e:
                self._set_last_error("request", f"AI request error: {e}", retryable=False)
                logger.warning("[AI] request error: %s", e)
                return None

            status = int(r.status_code)
            body_preview = (r.text or "").strip().replace("\n", " ")[:240]
            if status >= 400:
                if status in (401, 403):
                    self._set_last_error("auth", f"AI auth failed HTTP {status}: check api_key/permission", status_code=status, retryable=False)
                    logger.warning("[AI] auth failed HTTP %s | body=%s", status, body_preview)
                    return None
                if status == 404:
                    self._set_last_error("endpoint", f"AI endpoint/model not found HTTP 404", status_code=status, retryable=False)
                    logger.warning("[AI] endpoint/model not found HTTP 404 | url=%s | body=%s", url, body_preview)
                    return None
                if status == 429:
                    retryable = attempt < attempts
                    self._set_last_error("rate_limit", f"AI rate limited HTTP 429", status_code=status, retryable=retryable)
                    logger.warning("[AI] rate limited HTTP 429 (attempt %s/%s) | body=%s", attempt, attempts, body_preview)
                    if retryable:
                        wait_s = min(self.retry_max, self.retry_base * (2 ** (attempt - 1)))
                        ra = r.headers.get("Retry-After")
                        if ra:
                            try:
                                wait_s = min(self.retry_max, max(wait_s, float(ra)))
                            except Exception:
                                pass
                        time.sleep(wait_s)
                        continue
                    return None
                if 500 <= status <= 599:
                    retryable = attempt < attempts
                    self._set_last_error("server", f"AI upstream server error HTTP {status}", status_code=status, retryable=retryable)
                    logger.warning("[AI] upstream server error HTTP %s (attempt %s/%s) | body=%s", status, attempt, attempts, body_preview)
                    if retryable:
                        time.sleep(min(self.retry_max, self.retry_base * (2 ** (attempt - 1))))
                        continue
                    return None
                self._set_last_error("http", f"AI HTTP {status}", status_code=status, retryable=False)
                logger.warning("[AI] HTTP %s | body=%s", status, body_preview)
                return None

            try:
                data = r.json()
            except Exception as e:
                self._set_last_error("bad_json", f"AI response is not valid JSON: {e}", status_code=status, retryable=False)
                logger.warning("[AI] invalid response JSON: %s | body=%s", e, body_preview)
                return None
            try:
                content = data["choices"][0]["message"]["content"]
            except Exception:
                self._set_last_error("bad_payload", "AI response missing choices[0].message.content", status_code=status, retryable=False)
                logger.warning("[AI] response missing content | data=%s", str(data)[:240])
                return None
            parsed = self._parse_json_from_text(content)
            if parsed is None:
                self._set_last_error("bad_content", "AI content did not contain valid JSON object", status_code=status, retryable=False)
                logger.warning("[AI] content is not parseable JSON | content=%s", str(content)[:240])
                return None
            self.last_error = None
            return parsed
        return None


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


@dataclasses.dataclass
class SeriesMeta:
    tv_id: int
    name: str
    year: Optional[int]
    # season hint derived from original folder name, e.g. “第四季”
    season_hint: Optional[int] = None
    category: Optional[str] = None
    region: Optional[str] = None
    media_type: Optional[str] = None
    source_language: Optional[str] = None
    keywords: Optional[List[str]] = None
    ai_inferred: bool = False
    tmdb_confident: bool = True


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


def build_prefixed_sxxeyy_name(series_name: str, season: int, episode: int, old_filename: str) -> str:
    """Rename 'S01E01*.ext' -> '<series> - S01E01*.ext' (preserve suffix after SxxEyy)."""
    old_base, ext = os.path.splitext(old_filename)
    old_base = os.path.basename(old_base)
    old_base2 = re.sub(r"^\[[^\]]+\]\s*", "", old_base).strip()
    m = SXXEYY_RE.search(old_base2)
    remainder = ""
    if m:
        remainder = old_base2[m.end():]
    remainder = normalize_quality_tail(remainder.rstrip())
    if remainder and remainder[0] not in " ._-":
        remainder = " - " + remainder
    new_base = f"{series_name} - S{season:02d}E{episode:02d}{remainder}"
    new_base = re.sub(r"\s+", " ", new_base).strip()
    return safe_filename(new_base) + ext


def related_sidecars(entries: List[DirEntry], video_name: str, season: int, episode: int) -> List[str]:
    vstem, _ = os.path.splitext(video_name)
    tokens = {
        vstem.lower(),
        f"s{season:02d}e{episode:02d}",
        f"e{episode:02d}",
        f"{episode:02d}",
    }
    out: List[str] = []
    for e in entries:
        if e.is_dir:
            continue
        _, ext = os.path.splitext(e.name)
        if ext.lower() not in SUB_EXTS:
            continue
        stem = os.path.splitext(e.name)[0].lower()
        if stem in tokens:
            out.append(e.name)
            continue
        if f"s{season:02d}e{episode:02d}" in stem or f"e{episode:02d}" in stem:
            out.append(e.name)
            continue
        if re.match(rf"^\s*{episode:02d}\b", stem):
            out.append(e.name)
            continue
    return sorted(set(out))



_RES_RE = re.compile(r"(?i)\b(4320|2160|1440|1080|720|576|540|480)p\b")

def extract_resolution(text: str) -> str:
    """Extract canonical resolution string like '2160p'. 
    Rules:
      - Prefer explicit ####p.
      - If only '4K'/'UHD' appears -> 2160p
      - If only '8K' appears -> 4320p
      - Otherwise: '' (do not write anything)
    """
    if not text:
        return ""
    s = str(text)
    m = _RES_RE.search(s)
    if m:
        return f"{int(m.group(1))}p"
    low = to_halfwidth(s).lower()
    if "8k" in low:
        return "4320p"
    if "4k" in low or "uhd" in low:
        return "2160p"
    return ""


def build_new_video_name(series: str, season: int, episode: int, old_name: str, suffix: str) -> str:
    """Canonical episode filename.

    Desired format:
      - "{series} - S01E02 - 2160p.ext"  (when resolution detected)
      - "{series} - S01E02.ext"          (when resolution not detected)
    Notes:
      - 'series' should already include year when available, e.g. "鹿鼎记 (1998)".
      - We intentionally DO NOT keep codec/audio/source tags (h265/aac/web-dl/...).
    """
    _, ext = os.path.splitext(old_name)
    base = f"{series} - S{season:02d}E{episode:02d}"

    res = extract_resolution(f"{old_name} {suffix}")
    if res:
        base += f" - {res}"

    base = safe_filename(normalize_spaces(base))
    return base + ext



def ensure_dir(client: AlistClient, parent: str, name: str, dry_run: bool, log: List[str], assume_exists: bool = False) -> str:
    parent = norm_path(parent)
    name = safe_filename(name)
    target = join_path(parent, name)
    if assume_exists:
        if dry_run and target != parent:
            log.append(f"[DRY] mkdir {target}")
        return target
    entries = client.list_dir(parent)
    if any(e.is_dir and e.name == name for e in entries):
        return target
    if dry_run:
        log.append(f"[DRY] mkdir {target}")
        return target
    log.append(f"mkdir {target}")
    client.mkdir(target)
    return target


def maybe_rename_path(client: AlistClient, full_path: str, new_name: str, dry_run: bool, log: List[str], dry_return_new: bool = True, undo: 'UndoLogger|None' = None) -> str:
    parent, old = split_path(full_path)
    new_name = safe_filename(new_name)

    # IMPORTANT: if the name is already correct, do NOTHING.
    #
    # We must check this BEFORE conflict resolution.
    # Otherwise, the "exists" check will always see the file itself
    # and incorrectly rename it to "(1)".
    if old == new_name or not old:
        return full_path

    # avoid name collision in target directory
    resolved = unique_name_in_parent(client, parent, new_name)
    if not resolved:
        log.append(f"[SKIP] conflict: {full_path} -> {new_name} (exists)")
        return full_path
    if resolved != new_name:
        log.append(f"[INFO] conflict: {new_name} exists, use {resolved}")
        new_name = resolved
    if dry_run:
        log.append(f"[DRY] rename {full_path} -> {new_name}")
        # 预演模式：
        # - 对文件改名：返回“新路径”以便后续推导字幕/旁挂文件的新名字
        # - 对关键目录（例如剧根目录）改名：可选择返回旧路径，避免后续 list_dir 调用新路径导致 object not found
        return join_path(parent, new_name) if dry_return_new else full_path
    log.append(f"rename {full_path} -> {new_name}")
    client.rename(full_path, new_name)
    if undo:
        undo.record({"op": "rename_path", "parent": parent, "old": old, "new": new_name, "ts": now_ts()})
    return join_path(parent, new_name)


def maybe_rename(client: AlistClient, parent: str, old_name: str, new_name: str, dry_run: bool, log: List[str], undo: 'UndoLogger|None' = None) -> str:
    """Rename an item under `parent`.

    This is a small wrapper around maybe_rename_path() used by some code paths
    (e.g. episode-folder mode).  It also inherits the important *self-rename*
    guard to prevent the annoying "(1)" suffix bug.
    """
    full_path = join_path(parent, old_name)
    return maybe_rename_path(client, full_path, new_name, dry_run, log, dry_return_new=True, undo=undo)


def maybe_move(client: AlistClient, src_dir: str, dst_dir: str, names: List[str], dry_run: bool, log: List[str], undo: 'UndoLogger|None' = None):
    """Move items with basic conflict handling.

    If destination already has same name:
      - ON_CONFLICT=suffix: rename source item in-place (adds " (1)" ...) then move
      - ON_CONFLICT=skip: skip that item
    """
    if not names:
        return
    if norm_path(src_dir) == norm_path(dst_dir):
        return
    move_individual = bool(CURRENT_RUNTIME_CONFIG.get("move_individual", True))
    if dry_run:
        log.append(f"[DRY] move {names} : {src_dir} -> {dst_dir}")
        return

    if not move_individual:
        log.append(f"move {names} : {src_dir} -> {dst_dir}")
        client.move(src_dir, dst_dir, names)
        if undo:
            undo.record({"op": "move", "src_dir": src_dir, "dst_dir": dst_dir, "names": names, "ts": now_ts()})
        return

    # individual moves with conflict resolution
    try:
        dst_entries = client.list_dir(dst_dir, refresh=False)
        dst_existing = {e.name for e in dst_entries}
    except Exception:
        dst_existing = set()

    for name in list(names):
        if not name:
            continue
        final_name = name
        if final_name in dst_existing:
            resolved = unique_name_in_parent(client, dst_dir, final_name)
            if not resolved:
                log.append(f"[SKIP] move conflict: {join_path(src_dir, final_name)} -> {dst_dir}/{final_name}")
                continue
            if resolved != final_name:
                log.append(f"[INFO] move conflict: {final_name} exists in dst, rename src -> {resolved}")
                client.rename(join_path(src_dir, final_name), resolved)
                if undo:
                    undo.record({"op": "rename_path", "parent": src_dir, "old": final_name, "new": resolved, "ts": now_ts()})
                final_name = resolved
        log.append(f"move [{final_name}] : {src_dir} -> {dst_dir}")
        client.move(src_dir, dst_dir, [final_name])
        if undo:
            undo.record({"op": "move", "src_dir": src_dir, "dst_dir": dst_dir, "names": [final_name], "ts": now_ts()})
        dst_existing.add(final_name)


def maybe_move_folder_to_dir(
    client: AlistClient,
    folder_path: str,
    dst_dir: str,
    dry_run: bool,
    log: List[str],
    undo: 'UndoLogger|None' = None,
) -> str:
    """Move an entire folder to a destination directory (and resolve name conflicts).

    Returns the final folder path (predicted in dry-run).
    """
    folder_path = norm_path(folder_path)
    dst_dir = norm_path(dst_dir)

    src_parent, name = split_path(folder_path)
    if not name:
        return folder_path
    if norm_path(src_parent) == dst_dir:
        return folder_path

    original_folder_path = folder_path

    # Resolve conflict at destination
    final_name = unique_name_in_parent(client, dst_dir, name)
    if final_name != name:
        renamed = maybe_rename_path(client, folder_path, final_name, dry_run, log, undo=undo)
        folder_path = renamed
        src_parent, name = split_path(folder_path)

    maybe_move(client, src_parent, dst_dir, [name], dry_run, log, undo=undo)
    report_empty_dir(client, original_folder_path, getattr(log, "hub", None), dry_run=dry_run)
    return join_path(dst_dir, name)


def is_season_container_folder(name: str) -> bool:
    return bool(re.search(r"(?i)\bS\d{1,2}\s*-\s*S\d{1,2}\b", name))




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
DEFAULT_SKIP_DIR_REGEX = r"(福利|广告|推广|促销|活动|限时福利|限时|UC官方|阿里|Promo|sample|Samples?|Extras?|花絮|特典|周边|海报|Poster|封面|截图|Thumbs|@eaDir|\\.sync|lost\\+found|电影|Movie|剧场版|MOVIE)"




def apply_undo(client: AlistClient, undo_file: str, hub: 'LogHub|None' = None, yes: bool = False):
    """Rollback operations recorded in undo jsonl (reverse order).

    Supported ops:
      - rename_path: {op, parent, old, new}
      - move: {op, src_dir, dst_dir, names}

    This will best-effort apply; failures are logged and continue.
    """
    undo_file = (undo_file or '').strip()
    if not undo_file:
        raise ValueError('undo_file is empty')
    if not Path(undo_file).exists():
        raise FileNotFoundError(undo_file)
    if not yes:
        raise RuntimeError('Refuse to undo without --yes (safety).')

    # read records
    recs=[]
    with open(undo_file, 'r', encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            try:
                import json
                recs.append(json.loads(line))
            except Exception:
                continue

    def emit(level, msg):
        if hub:
            hub.emit(level, msg)
        else:
            print(f"{level}: {msg}")

    emit('INFO', f"[UNDO] loaded {len(recs)} records from {undo_file}")

    for rec in reversed(recs):
        op = rec.get('op')
        try:
            if op == 'rename_path':
                parent = rec.get('parent')
                old = rec.get('old')
                new = rec.get('new')
                if parent and old and new:
                    emit('INFO', f"[UNDO] rename {join_path(parent, new)} -> {old}")
                    client.rename(join_path(parent, new), old)
            elif op == 'move':
                src_dir = rec.get('src_dir')
                dst_dir = rec.get('dst_dir')
                names = rec.get('names') or []
                if src_dir and dst_dir and names:
                    emit('INFO', f"[UNDO] move {names} : {dst_dir} -> {src_dir}")
                    client.move(dst_dir, src_dir, list(names))
            else:
                continue
        except Exception as e:
            emit('ERROR', f"[UNDO] failed {op}: {e}")

    emit('INFO', '[UNDO] done')
def should_skip_misc_folder(name: str, user_regex: str = "") -> bool:
    """Skip obvious non-media / promo / junk folders.

    - Default patterns cover: 福利/广告/活动/Promo/Sample/Extras/海报/花絮 等
    - You can override/extend via env SKIP_DIR_REGEX or CLI --skip-dir-regex
    """
    rx = user_regex.strip() or str(CURRENT_RUNTIME_CONFIG.get("skip_dir_regex", "") or "").strip() or DEFAULT_SKIP_DIR_REGEX
    try:
        if re.search(rx, name, flags=re.IGNORECASE):
            return True
    except re.error:
        # fallback to contains check if regex invalid
        pass

    # conservative contains fallback
    bad = ["福利", "广告", "推广", "活动", "限时", "promo", "sample", "extras", "海报", "花絮", "封面", "截图"]
    return any(x.lower() in name.lower() for x in bad)


def contains_junk_marker(name: str) -> bool:
    nl = (name or "").lower()
    return any(m.lower() in nl for m in JUNK_MARKERS)

def is_subtitle_dir_name(name: str) -> bool:
    nl = (name or "").strip().lower()
    return nl in SUBTITLE_DIR_NAMES

def cleanup_ads_in_dir(client: AlistClient, dir_path: str, hub: Optional[LogHub], dry_run: bool = False):
    """Best-effort cleanup of obvious ad/junk files or folders.

    * Won't delete .txt (user requirement)
    * Won't delete subtitle directories (we relocate subtitles elsewhere)
    * Won't delete season directories
    """
    if not bool_env("DELETE_ADS", True):
        return
    try:
        entries = client.list_dir(dir_path, refresh=False)
    except Exception as e:
        logger.warning("[WARN] cleanup listdir failed: %s : %s", dir_path, e)
        return

    del_files: List[str] = []
    del_dirs: List[str] = []

    for ent in entries:
        name = ent.name
        if ent.is_dir:
            if is_subtitle_dir_name(name):
                continue
            if is_season_dir(name):
                continue
            # remove obvious junk folders
            if name.lower().strip() in {"@eadir", "__macosx"} or contains_junk_marker(name):
                del_dirs.append(name)
        else:
            ext = os.path.splitext(name)[1].lower()
            if ext == ".txt":
                continue
            if ext in AD_DELETE_EXTS:
                del_files.append(name)
            # Also remove tiny "ad" images/html by marker in name
            elif contains_junk_marker(name) and ext not in {".nfo", ".jpg", ".jpeg", ".png", ".webp"}:
                del_files.append(name)

    def _emit(level: str, msg: str):
        if hub:
            try:
                hub.emit(level, msg)
            except Exception:
                pass

    if del_files:
        _emit("INFO", f"[CLEAN] remove files: {len(del_files)}")
        if not dry_run:
            client.remove(dir_path, del_files)

    if del_dirs:
        _emit("INFO", f"[CLEAN] remove dirs: {len(del_dirs)}")
        if not dry_run:
            client.remove(dir_path, del_dirs)


def report_empty_dir(client: AlistClient, dir_path: str, hub: Optional[LogHub], dry_run: bool = False):
    """Report an empty directory without deleting it."""
    dir_path = norm_path(dir_path)
    if not dir_path or dir_path in {"/", "."}:
        return

    try:
        entries = client.list_dir(dir_path, refresh=False)
    except Exception:
        return

    if entries:
        return

    msg = f"[EMPTY] folder left empty: {dir_path}"
    if dry_run:
        msg = f"[DRY] {msg}"
    if hub:
        try:
            hub.emit("WARN", msg)
            return
        except Exception:
            pass
    logger.warning(msg)


def build_season_dir_map(client: AlistClient, series_path: str) -> Dict[int, str]:
    """Map season number -> season directory path for a show folder."""
    m: Dict[int, str] = {}
    try:
        for ent in client.list_dir(series_path, refresh=False):
            if not ent.is_dir:
                continue
            s = is_season_dir(ent.name)
            if s:
                m[s] = join_path(series_path, ent.name)
    except Exception:
        return m
    return m


def relocate_subtitles_in_show_root(
    client: AlistClient,
    series_path: str,
    log: List[str],
    dry_run: bool,
    season_fmt: str,
):
    """Handle /Show/字幕 (or /Show/subs) kind of layouts.

    We move subtitle files to the same directory as the corresponding video files (season dir),
    so later rename logic can match them and avoid name conflicts.
    """
    try:
        entries = client.list_dir(series_path, refresh=False)
    except Exception:
        return

    season_map = build_season_dir_map(client, series_path)

    # find subtitle directories at show root
    for ent in entries:
        if not ent.is_dir:
            continue
        if not is_subtitle_dir_name(ent.name):
            continue

        sub_dir = join_path(series_path, ent.name)

        def _emit(msg: str):
            try:
                log.append(msg)
            except Exception:
                pass

        _emit(f"[SUB] relocate subtitles from {ent.name}")
        try:
            sub_entries = client.list_dir(sub_dir, refresh=False)
        except Exception:
            continue

        # allow one level nested season folders inside subtitle dir
        candidate_dirs: List[Tuple[Optional[int], str]] = [(None, sub_dir)]
        for se in sub_entries:
            if se.is_dir:
                s = is_season_dir(se.name)
                if s:
                    candidate_dirs.append((s, join_path(sub_dir, se.name)))

        for s_hint, src_dir in candidate_dirs:
            try:
                files = [e.name for e in client.list_dir(src_dir, refresh=False) if (not e.is_dir)]
            except Exception:
                continue
            for fn in files:
                ext = os.path.splitext(fn)[1].lower()
                if ext not in SUB_EXTS and ext not in SUBTITLE_EXTS:
                    continue

                # infer season/episode from subtitle filename
                s, ep, _already, _suffix = parse_episode_from_name(fn)
                if s is None:
                    s = s_hint
                # if still unknown and only one season dir exists, use it
                if s is None and len(season_map) == 1:
                    s = next(iter(season_map.keys()))
                if s is None:
                    s = 1

                dst_dir = season_map.get(s)
                if not dst_dir:
                    # create standard season dir (best-effort)
                    dst_name = season_folder_name(s, season_fmt)
                    dst_dir = join_path(series_path, dst_name)
                    try:
                        client.mkdir(dst_dir)
                    except Exception:
                        pass
                    season_map[s] = dst_dir

                maybe_move(client, src_dir, dst_dir, [fn], log, dry_run, undo=None)

def parse_csv_paths(csv_value: str) -> List[str]:
    """Parse comma-separated paths into normalized AList paths."""
    items: List[str] = []
    text = (csv_value or "").replace("，", ",")
    for raw in text.split(","):
        p = raw.strip()
        if not p:
            continue
        items.append(norm_path(p))
    return items



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
def process_series_folder(
    client: AlistClient,
    tmdb: TMDBClient,
    ai: Optional[AIClient],
    series_path: str,
    season_fmt: str,
    rename_series: bool,
    rename_files: bool,
    fix_bare_sxxeyy: bool,
    dry_run: bool,
    cache: Dict[str, Any],
    log: List[str],
    skip_dir_regex: str,
    undo: Optional[UndoLogger] = None,
    library_roots: Optional[List[str]] = None,
    depth: int = 0,
) -> Tuple[str, Optional[SeriesMeta]]:
    series_path = norm_path(series_path)
    _, folder_name = split_path(series_path)

    # Guard against accidental deep recursion (e.g. badly nested collections).
    if depth > 3:
        log.append(f"[SKIP] nesting too deep ({depth}): {series_path}")
        return series_path, None

    library_roots = library_roots or []
    lib_root = find_library_root(series_path, library_roots) or os.path.dirname(series_path)

    # Pre-scan to detect "container/collection" folders (e.g. "鬼吹灯全系列") and improve TMDB matching.
    try:
        root_entries_pre = client.list_dir(series_path, refresh=False)
    except Exception as e:
        log.append(f"[ERROR] list_dir failed: {series_path} ({e})")
        return series_path, None

    video_at_root = any((not e.is_dir and os.path.splitext(e.name)[1].lower() in VIDEO_EXTS) for e in root_entries_pre)
    season_dir_count = sum(1 for e in root_entries_pre if e.is_dir and parse_season_from_text(e.name) is not None)
    child_dirs = [
        e for e in root_entries_pre
        if e.is_dir and parse_season_from_text(e.name) is None and (e.name not in CATEGORY_CONTAINER_NAMES) and (not should_skip_misc_folder(e.name, skip_dir_regex))
    ]
    show_like_child_dirs = [e for e in child_dirs if looks_like_show_folder_name(e.name)]
    container_words = re.search(r"(全系列|系列|合集|全套|全集|collection|franchise)", folder_name, re.I) is not None

    def _dir_has_video_within(path: str, max_depth: int = 2) -> bool:
        """Cheap bounded probe for videos below a child show folder."""
        if max_depth < 0:
            return False
        try:
            entries = client.list_dir(path, refresh=False)
        except Exception:
            return False
        for ent in entries:
            if not ent.is_dir and os.path.splitext(ent.name)[1].lower() in VIDEO_EXTS:
                return True
        if max_depth == 0:
            return False
        for ent in entries:
            if ent.is_dir and (not should_skip_misc_folder(ent.name, skip_dir_regex)):
                if _dir_has_video_within(join_path(path, ent.name), max_depth - 1):
                    return True
        return False

    child_dirs_with_video = []
    for d in child_dirs:
        child_path = join_path(series_path, d.name)
        if _dir_has_video_within(child_path, max_depth=2):
            child_dirs_with_video.append(d)

    if (not video_at_root) and season_dir_count == 0 and (
        len(show_like_child_dirs) >= 2
        or (container_words and len(child_dirs) >= 2)
        or (len(child_dirs_with_video) >= 1 and (container_words or re.search(r"(动漫|动画|合集|系列|全集)", folder_name, re.I)))
    ):
        log.append(f"[INFO] detected collection container: {series_path} | child_dirs={len(child_dirs)} | show_like={len(show_like_child_dirs)} | child_with_video={len(child_dirs_with_video)}")
        dirs_to_process = show_like_child_dirs if show_like_child_dirs else (child_dirs_with_video if child_dirs_with_video else child_dirs)
        for d in dirs_to_process:
            child_path = join_path(series_path, d.name)
            child_result = process_series_folder(
                client=client,
                tmdb=tmdb,
                ai=ai,
                series_path=child_path,
                season_fmt=season_fmt,
                rename_series=rename_series,
                rename_files=rename_files,
                fix_bare_sxxeyy=fix_bare_sxxeyy,
                dry_run=dry_run,
                cache=cache,
                log=log,
                skip_dir_regex=skip_dir_regex,
                undo=undo,
                library_roots=library_roots,
                depth=depth + 1,
            )
            processed_child, _child_meta = child_result if isinstance(child_result, tuple) else (child_path, None)
            try:
                maybe_move_folder_to_dir(client, processed_child, lib_root, dry_run, log, undo=undo)
            except Exception as e:
                log.append(f"[ERROR] move nested show to root failed: {processed_child} -> {lib_root} ({e})")
        return series_path, None

    series_context = gather_series_context(client, series_path, skip_dir_regex)
    meta = resolve_series(tmdb, folder_name, cache, ai, log, series_context=series_context)
    if not meta:
        log.append(f"[SKIP] TMDB not found for: {series_path}")
        return series_path, None

    desired_series_name = meta.name
    if meta.year:
        desired_series_name = f"{meta.name} ({meta.year})"

    new_series_path = series_path
    if rename_series:
        new_series_path = maybe_rename_path(client, series_path, desired_series_name, dry_run, log, dry_return_new=False, undo=undo)

    hub = getattr(log, "hub", None)
    cleanup_ads_in_dir(client, new_series_path, hub, dry_run=dry_run)
    relocate_subtitles_in_show_root(client, new_series_path, log, dry_run, season_fmt)

    root_entries = client.list_dir(new_series_path)

    # 1) flatten season-container folders like "S1-S3"
    for e in list(root_entries):
        if e.is_dir and is_season_container_folder(e.name):
            container_path = join_path(new_series_path, e.name)
            sub_entries = client.list_dir(container_path)
            season_dirs = [se for se in sub_entries if se.is_dir and parse_season_from_text(se.name) is not None]
            for sd in season_dirs:
                maybe_move(client, container_path, new_series_path, [sd.name], dry_run, log, undo=undo)
            root_entries = client.list_dir(new_series_path)
            break

    def ensure_season_dir(season: int) -> str:
        return ensure_dir(client, new_series_path, season_folder_name(season, season_fmt), dry_run, log)

    # Keep a hint of the ORIGINAL season folder names (some users put "1080P/4K" in them).
    # We may rename those folders to "Sxx" later, but still want to use the hint for resolution.
    season_dir_hints: Dict[int, str] = {}

    # 2) normalize existing season dir names
    root_entries = client.list_dir(new_series_path)
    for e in root_entries:
        if not e.is_dir:
            continue
        season = parse_season_from_text(e.name)
        if season is None:
            continue
        desired = season_folder_name(season, season_fmt)
        if e.name != desired:
            maybe_rename_path(client, join_path(new_series_path, e.name), desired, dry_run, log, undo=undo)

    root_entries = client.list_dir(new_series_path)
    season_dir_names = {e.name for e in root_entries if e.is_dir and parse_season_from_text(e.name) is not None}

    # 3) scan root + one-level misc folders (quality folders etc) to merge scattered seasons
    #    BUT do NOT treat nested "show folders" (e.g. a franchise collection) as seasons.
    scan_dirs: List[str] = [new_series_path]
    nested_show_dirs: List[str] = []
    for e in root_entries:
        if not e.is_dir:
            continue
        if e.name in season_dir_names:
            continue
        if should_skip_misc_folder(e.name, skip_dir_regex):
            continue

        # If this directory looks like a separate show (has year / "全xx集" etc),
        # keep it aside and process it later as its own show.
        if looks_like_show_folder_name(e.name) and not is_same_show_container_folder(e.name, desired_series_name):
            nested_show_dirs.append(join_path(new_series_path, e.name))
            continue

        scan_dirs.append(join_path(new_series_path, e.name))

    # season hint for root, derived from original folder name (before renaming)
    root_season_hint = meta.season_hint

    # Default season fallback (most TV/variety libraries assume season 1)
    default_season = int(CURRENT_RUNTIME_CONFIG.get("default_season", 1) or 1)

    # Build per-scan-dir season hints for variety inference.
    # (We cannot rely on a global `incoming_scan_season_hints` here.)
    scan_season_hints: Dict[str, int] = {}
    for sd in scan_dirs:
        base = os.path.basename(sd).strip()
        sh = parse_season_from_text(base)
        if sh is not None:
            scan_season_hints[sd] = sh
        elif sd == new_series_path and (root_season_hint is not None):
            scan_season_hints[sd] = int(root_season_hint)

    # Infer episodes for variety shows when filenames lack episode numbers (date-only / 第X期上/下).
    # Also classify Specials (抢先看/花絮/特辑...) into season 0.
    variety_plans = infer_variety_and_special_episodes(
        client=client,
        scan_dirs=scan_dirs,
        incoming_scan_season_hints=scan_season_hints,
        default_season=default_season,
    )

    for scan_dir in scan_dirs:
        entries = client.list_dir(scan_dir)
        _, scan_basename = split_path(scan_dir)
        scan_season_hint = parse_season_from_text(scan_basename) or (root_season_hint if scan_dir == new_series_path else None)

        # Season fallback policy:
        # - If file name has no explicit season marker (Sxx / 第X季 / Season X), we DO NOT call AI.
        # - Most CN dramas use "E01/E02/..." to mean episode numbers, not seasons.
        # - Default season can be overridden by env DEFAULT_SEASON (default: 1).
        default_season = int(CURRENT_RUNTIME_CONFIG.get("default_season", 1) or 1)
        allow_ai_infer_season = bool(CURRENT_RUNTIME_CONFIG.get("ai_infer_season", False))
        # By default we CANONICALIZE existing SxxEyy filenames (year + resolution-only tail).
        # Set PROTECT_SXXEYY=1 to keep old behavior (never rename when SxxEyy already present).
        protect_sxxeyy = bool(CURRENT_RUNTIME_CONFIG.get("protect_sxxeyy", False))


        def _has_any_season_hint(*texts: str) -> bool:
            joined = " ".join([t for t in texts if t])
            if not joined:
                return False
            # Quick conservative hint detection (do not over-trigger).
            return bool(
                re.search(r"第\s*[一二三四五六七八九十\d]+\s*季", joined)
                or re.search(r"(?i)\bSeason\s*\d{1,2}\b", joined)
                # Standalone S01 in folder context (avoid matching S01E02 in filenames)
                or re.search(r"(?i)(?:^|\W)S(\d{1,2})(?:$|\W)", joined)
            )

        for ent in entries:
            # Graceful stop (triggered via web UI /api/stop)
            if hasattr(log, "hub") and getattr(getattr(log, "hub", None), "stop_requested", None):
                if log.hub.stop_requested():
                    log.append("[STOP] series processing stopped by user request")
                    return new_series_path, meta
            if ent.is_dir:
                # “每集一个文件夹”结构：<series>/<S04E01>/<video+subs>
                s_dir, ep_dir, _, _ = parse_episode_from_name(ent.name)
                if ep_dir is None:
                    continue

                season_dir_num = s_dir or scan_season_hint or default_season
                dst_season_path = ensure_season_dir(season_dir_num)
                ep_folder_path = join_path(scan_dir, ent.name)
                ep_entries = client.list_dir(ep_folder_path)

                for f in ep_entries:
                    if f.is_dir:
                        continue
                    _, ext = os.path.splitext(f.name)
                    if ext.lower() not in VIDEO_EXTS:
                        continue

                    s2, ep2, already2, suffix2 = parse_episode_from_name(f.name)
                    # folder gave us ep number; if file doesn't, trust folder.
                    season_num = s2 or season_dir_num
                    ep_num = ep2 or ep_dir

                    sidecars = related_sidecars(ep_entries, f.name, season_num, ep_num)
                    if norm_path(ep_folder_path) != norm_path(dst_season_path):
                        maybe_move(client, ep_folder_path, dst_season_path, [f.name] + sidecars, dry_run, log, undo=undo)

                    if rename_files and (((not already2) or (not CURRENT_RUNTIME_CONFIG.get('protect_sxxeyy', True))) or contains_junk_marker(f.name) or (fix_bare_sxxeyy and already2 and needs_series_prefix_for_sxxeyy(f.name, desired_series_name))):
                        new_video = (
                        build_prefixed_sxxeyy_name(desired_series_name, season_num, ep_num, f.name)
                        if (already2 and fix_bare_sxxeyy and needs_series_prefix_for_sxxeyy(f.name, desired_series_name))
                        else build_new_video_name(
                            desired_series_name,
                            season_num,
                            ep_num,
                            f.name,
                            " ".join([suffix2, scan_basename, folder_name]).strip(),
                        )
                    )
                        maybe_rename(client, dst_season_path, f.name, new_video, dry_run, log)
                        for sc in sidecars:
                            new_stem = os.path.splitext(new_video)[0]
                            new_sc = build_new_sidecar_name(new_stem, sc, season_num, ep_num)
                            maybe_rename(client, dst_season_path, sc, new_sc, dry_run, log)

                continue
            _, ext = os.path.splitext(ent.name)
            if ext.lower() not in VIDEO_EXTS:
                continue

            s, ep, already, suffix = parse_episode_from_name(ent.name)

            special = False
            if ep is None:
                plan = variety_plans.get((scan_dir, ent.name))
                if plan:
                    season, ep, special = plan
                else:
                    continue
            else:
                season = s
                if season is None:
                    season = scan_season_hint

            # If still unknown season, optionally allow AI inference.
            # IMPORTANT: Only enable via env AI_INFER_SEASON=1 AND there is an explicit season-like hint.
            # Otherwise, titles like "E11" (episode 11) get misread as Season 11.
            if season is None and ai and allow_ai_infer_season and _has_any_season_hint(folder_name, scan_basename):
                system = "You infer TV episode season number from context. Output JSON only."
                user = (
                    "We are organizing a TV series library. Given the folder name context and file name, infer season number. "
                    "Return JSON: {\"season\": number|null}. Use null if unsure.\n\n"
                    f"series_folder_original: {folder_name}\n"
                    f"scan_folder: {scan_basename}\n"
                    f"video_file: {ent.name}\n"
                )
                js = ai.chat_json(system, user)
                if js and isinstance(js.get("season"), (int, float)):
                    season = int(js["season"])
                    log.append(f"[AI] inferred season={season} for {ent.name} in {scan_basename}")

            if season is None:
                season = default_season

            dst_season_path = ensure_season_dir(season)
            sidecars = related_sidecars(entries, ent.name, season, ep)

            # move video + sidecars
            if norm_path(scan_dir) != norm_path(dst_season_path):
                maybe_move(client, scan_dir, dst_season_path, [ent.name] + sidecars, dry_run, log, undo=undo)

            # rename video:
            # - default: never touch filenames that already contain SxxEyy
            # - exception (optional): if filename is bare 'SxxEyy*' without series title, prefix series name.
            if rename_files:
                if ((not already) or (not protect_sxxeyy)):
                    suffix_ctx = " ".join([suffix, scan_basename, folder_name]).strip()
                    new_video = build_new_video_name(desired_series_name, season, ep, ent.name, suffix_ctx)
                    new_full = maybe_rename_path(client, join_path(dst_season_path, ent.name), new_video, dry_run, log, undo=undo)
                    new_stem = os.path.splitext(os.path.basename(new_full))[0]
                elif fix_bare_sxxeyy and needs_series_prefix_for_sxxeyy(ent.name, desired_series_name):
                    new_video = build_prefixed_sxxeyy_name(desired_series_name, season, ep, ent.name)
                    new_full = maybe_rename_path(client, join_path(dst_season_path, ent.name), new_video, dry_run, log, undo=undo)
                    new_stem = os.path.splitext(os.path.basename(new_full))[0]
                else:
                    new_stem = os.path.splitext(ent.name)[0]

            # sidecars follow final video stem
            for sc in sidecars:
                new_sc_name = safe_filename(build_new_sidecar_name(new_stem, sc, season, ep))
                maybe_rename_path(client, join_path(dst_season_path, sc), new_sc_name, dry_run, log, undo=undo)

    # 4) inside each season dir: rename videos lacking SxxEyy and fix sidecars
    root_entries = client.list_dir(new_series_path)
    for e in root_entries:
        if not e.is_dir:
            continue
        season = parse_season_from_text(e.name)
        if season is None:
            continue
        season_path = join_path(new_series_path, e.name)
        cleanup_ads_in_dir(client, season_path, hub, dry_run=dry_run)
        entries = client.list_dir(season_path)
        for ent in entries:
            # Graceful stop (triggered via web UI /api/stop)
            if hasattr(log, "hub") and getattr(getattr(log, "hub", None), "stop_requested", None):
                if log.hub.stop_requested():
                    log.append("[STOP] series processing stopped by user request")
                    return new_series_path, meta
            if ent.is_dir:
                continue
            _, ext = os.path.splitext(ent.name)
            if ext.lower() not in VIDEO_EXTS:
                continue
            s, ep, already, suffix = parse_episode_from_name(ent.name)
            if ep is None:
                continue
            if s is None:
                s = season
            sidecars = related_sidecars(entries, ent.name, s, ep)

            if rename_files and ((not already) or contains_junk_marker(ent.name)):
                season_hint_text = season_dir_hints.get(s) or season_dir_hints.get(season) or ""
                suffix_ctx = " ".join([suffix, season_hint_text, os.path.basename(season_path), folder_name]).strip()
                new_video = build_new_video_name(desired_series_name, s, ep, ent.name, suffix_ctx)
                new_full = maybe_rename_path(client, join_path(season_path, ent.name), new_video, dry_run, log, undo=undo)
                new_stem = os.path.splitext(os.path.basename(new_full))[0]
            elif rename_files and already and fix_bare_sxxeyy and needs_series_prefix_for_sxxeyy(ent.name, desired_series_name):
                new_video = build_prefixed_sxxeyy_name(desired_series_name, s, ep, ent.name)
                new_full = maybe_rename_path(client, join_path(season_path, ent.name), new_video, dry_run, log, undo=undo)
                new_stem = os.path.splitext(os.path.basename(new_full))[0]
            else:
                new_stem = os.path.splitext(ent.name)[0]

            for sc in sidecars:
                new_sc = safe_filename(build_new_sidecar_name(new_stem, sc, s, ep))
                maybe_rename_path(client, join_path(season_path, sc), new_sc, dry_run, log, undo=undo)


    # 5) Process nested show folders (spinoffs/collections) that were discovered inside this show folder.
    #    We intentionally did NOT scan them as episodes of the current show.
    if nested_show_dirs:
        log.append(f"[INFO] found nested show folders under {new_series_path}: {len(nested_show_dirs)}")
        for child_path in list(nested_show_dirs):
            try:
                nested_result = process_series_folder(
                    client=client,
                    tmdb=tmdb,
                    ai=ai,
                    series_path=child_path,
                    season_fmt=season_fmt,
                    rename_series=rename_series,
                    rename_files=rename_files,
                    fix_bare_sxxeyy=fix_bare_sxxeyy,
                    dry_run=dry_run,
                    cache=cache,
                    log=log,
                    skip_dir_regex=skip_dir_regex,
                    undo=undo,
                    library_roots=library_roots,
                    depth=depth + 1,
                )
                child_final, _nested_meta = nested_result if isinstance(nested_result, tuple) else (child_path, None)
                _ = maybe_move_folder_to_dir(client, child_final, lib_root, dry_run, log, undo=undo)
            except Exception as e:
                log.append(f"[ERROR] failed to process nested show folder {child_path}: {e}")

    report_empty_dir(client, new_series_path, hub, dry_run=dry_run)
    return new_series_path, meta



def _start_logui_if_needed(args, hub: LogHub):
    cfg_log_web = bool(CURRENT_RUNTIME_CONFIG.get("log_web", False))
    if not (getattr(args, "log_web", False) or cfg_log_web):
        return None
    try:
        srv = start_log_server(hub, host=args.log_host, port=int(args.log_port), token=(args.log_token or None))
        hub.emit("INFO", f"[LOGUI] http://{args.log_host}:{int(srv.port)} (bind={args.log_host})")
        return srv
    except Exception as e:
        if getattr(e, "errno", None) == 98:
            try:
                srv = start_log_server(hub, host=args.log_host, port=0, token=(args.log_token or None))
                hub.emit("INFO", f"[LOGUI] http://{args.log_host}:{int(srv.port)} (bind={args.log_host})")
                return srv
            except Exception as e2:
                hub.emit("ERROR", f"[ERROR] Failed to start log UI: {e2}")
                return None
        hub.emit("ERROR", f"[ERROR] Failed to start log UI: {e}")
        return None


def build_runtime_parser():
    ap = argparse.ArgumentParser(description="Batch organize/rename AList TV folders for Emby using TMDB (+ optional AI).")

    ap.add_argument("--alist-url", default="")
    ap.add_argument("--alist-token", default="")
    ap.add_argument("--alist-user", default="")
    ap.add_argument("--alist-pass", default="")
    ap.add_argument("--alist-otp", default="")
    ap.add_argument("--tmdb-key", default="")
    ap.add_argument("--tmdb-lang", default="zh-CN")
    ap.add_argument(
        "--roots",
        default="",
        help="Comma-separated library roots (only TV/anime roots; never point this to movies)",
    )
    ap.add_argument("--auto-roots", action="store_true", help="Auto-discover TV roots (电视剧/动漫) under OneDrive storages")
    ap.add_argument("--discover-root-regex", default=r"^OneDrive-")
    ap.add_argument("--discover-categories", default="电视剧,动漫")

    # 兼容旧参数：--only
    ap.add_argument("--keyword", dest="keyword", default="")
    ap.add_argument("--only", dest="keyword", default="")

    # 只做发现/搜索，不做改名（不需要 TMDB_KEY）
    ap.add_argument("--discover-only", action="store_true", help="Print discovered TV roots then exit")
    ap.add_argument("--search-only", default="", help="Search series folders by keyword then exit")

    ap.add_argument("--max-series", type=int, default=0, help="Limit number of series processed (0=unlimited)")
    ap.add_argument(
        "--season-format",
        default="S{season:02d}",
        help='Season folder format. Examples: "S{season:02d}" (default), "Season {season}".',
    )
    ap.add_argument("--no-rename-series", action="store_true")
    ap.add_argument("--no-rename-files", action="store_true")

    fix_default = True
    g_fix = ap.add_mutually_exclusive_group()
    g_fix.add_argument("--fix-bare-sxxeyy", dest="fix_bare_sxxeyy", action="store_true", help="Prefix series name for bare 'SxxEyy' filenames (e.g. S01E01.mkv).")
    g_fix.add_argument("--no-fix-bare-sxxeyy", dest="fix_bare_sxxeyy", action="store_false", help="Do not rename bare 'SxxEyy' filenames.")
    ap.set_defaults(fix_bare_sxxeyy=fix_default)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.8, help="Min seconds between AList requests")
    ap.add_argument("--tmdb-sleep", type=float, default=0.3)
    ap.add_argument("--cache", default="tmdb_cache.json")
    ap.add_argument("--insecure", action="store_true")

    # Skip junk folders (ads/promo/posters/extras/etc)
    ap.add_argument(
        "--skip-dir-regex",
        default="",
        help=(
            "Regex for folder names to skip while scanning (e.g. 福利/广告/活动/海报/花絮). "
            "If empty, a safe default is used."
        ),
    )

    # AI options (OpenAI-compatible)
    ap.add_argument("--ai-base-url", default="https://api.openai.com", help="OpenAI-compatible base URL")
    ap.add_argument("--ai-api-key", default="", help="API key")
    ap.add_argument("--ai-model", default="gpt-4o-mini", help="Model name")
    ap.add_argument("--ai-sleep", type=float, default=1.2, help="Min seconds between AI calls")
    ap.add_argument("--no-ai", action="store_true", help="Disable AI even if key is present")


    # Resume/Undo
    ap.add_argument("--state-file", default="", help="Path to state jsonl for resume.")
    resume_default = False
    g_res = ap.add_mutually_exclusive_group()
    g_res.add_argument("--resume", dest="resume", action="store_true", help="Skip series already marked done in state file")
    g_res.add_argument("--no-resume", dest="resume", action="store_false", help="Do not use resume state")
    ap.set_defaults(resume=resume_default)
    ap.add_argument("--undo-log", default="", help="Write undo jsonl log in apply mode")
    ap.add_argument("--undo", default="", help="Rollback using undo jsonl file then exit")
    ap.add_argument("--yes", action="store_true", help="Non-interactive confirm (for undo)")

    # Logging / Web UI
    ap.add_argument("--log-file", default="", help="Write run log to file (default: logs/embyrename-<timestamp>.log)")
    ap.add_argument("--log-web", action="store_true", help="Start a small web UI for real-time logs")
    ap.add_argument("--log-host", default="127.0.0.1", help="Log UI bind host (default: 127.0.0.1)")
    ap.add_argument("--log-port", type=int, default=55255, help="Log UI port (default: 55255)")
    ap.add_argument("--log-token", default="", help="Log UI token (optional). If set, UI requires ?token=...")
    ap.add_argument("--log-keep", type=int, default=500, help="How many recent lines to keep in memory for the log UI")
    ap.add_argument("--organize-enabled", action="store_true", help="Enable organized library mode that initializes target folders and moves processed folders into them")
    ap.add_argument("--target-root", default="", help="AList target root for organized library mode")
    ap.add_argument("--exclude-roots", default="", help="Comma/Chinese-comma separated folder roots to exclude from scanning")
    ap.add_argument("--scan-exclude-target", action="store_true", help="Do not scan target root during organized library mode")
    ap.add_argument("--init-target-tree", action="store_true", help="Create category/region folders under target root before scanning")
    ap.add_argument("--category-buckets", default="电影,剧集,动漫,纪录片,综艺,演唱会,体育", help="Category buckets used by the web UI; accepted for config compatibility")
    ap.add_argument("--region-buckets", default="大陆,港台,欧美,日韩,其他", help="Region buckets used by the web UI; accepted for config compatibility")
    ap.add_argument("--category-region-map", default="", help="JSON or '分类:地区1,地区2;分类2:地区A' mapping for organized library tree")
    ap.add_argument("--save-config", action="store_true", help="Persist CLI overrides into the shared WebUI config file before running")
    return ap


def run_job(args, *, cfg_dir: Optional[str] = None, store: Optional[RuntimeConfigStore] = None, start_webui: bool = True, hub: Optional[LogHub] = None):
    cfg_dir = cfg_dir or get_config_dir()
    cfg_logs = os.path.join(cfg_dir, "logs")

    try:
        os.makedirs(cfg_logs, exist_ok=True)
    except Exception:
        pass

    if (args.cache or "").strip() == "tmdb_cache.json":
        args.cache = os.path.join(cfg_dir, "tmdb_cache.json")

    if not (args.state_file or "").strip():
        args.state_file = os.path.join(cfg_logs, "state.jsonl")

    ts = time.strftime("%Y%m%d-%H%M%S")
    log_file = (args.log_file or "").strip() or os.path.join(cfg_logs, f"embyrename-{ts}.log")
    os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
    own_hub = hub is None
    hub = hub or LogHub(log_file=log_file, also_print=True, keep=args.log_keep)

    srv = _start_logui_if_needed(args, hub) if start_webui else None

    log = LiveLog(hub)

    cache: Dict[str, Any] = {}
    if os.path.exists(args.cache):
        try:
            with open(args.cache, "r", encoding="utf-8") as f:
                cache = json.load(f) or {}
        except Exception:
            cache = {}

    client = AlistClient(
        base_url=args.alist_url,
        token=args.alist_token or None,
        username=args.alist_user or None,
        password=args.alist_pass or None,
        otp_code=args.alist_otp or None,
        sleep=args.sleep,
        verify_tls=(not args.insecure),
        on_token_refresh=(
            (lambda token: store.save({'alist_token': token}))
            if store is not None else None
        ),
    )

    # Undo mode: rollback then exit (no TMDB needed)
    if (args.undo or "").strip():
        undo_file = args.undo.strip()
        apply_undo(client=client, undo_file=undo_file, hub=hub, yes=args.yes)
        hub.emit("INFO", "[DONE] Undo finished")
        if own_hub:
            hub.close()
        return

    # 2) Resolve library roots (TV/anime roots only)
    organize_enabled = parse_boolish(getattr(args, "organize_enabled", False))
    target_root = norm_path((getattr(args, "target_root", "") or "").strip())
    scan_exclude_target = parse_boolish(getattr(args, "scan_exclude_target", False))
    init_target_tree = parse_boolish(getattr(args, "init_target_tree", False))
    category_region_map = parse_category_region_map(getattr(args, "category_region_map", ""))
    roots = parse_csv_paths(args.roots)
    exclude_roots = [norm_path(p) for p in parse_csv_paths(getattr(args, "exclude_roots", "")) if norm_path(p)]
    excluded_root_set = set(exclude_roots)
    auto_roots = bool(getattr(args, "auto_roots", False))
    if (not roots) and auto_roots:
        roots = discover_library_roots(
            client=client,
            root_regex=args.discover_root_regex,
            categories_csv=args.discover_categories,
            ttl_days=7,
        )

    if organize_enabled:
        organize_roots = list(roots)
        if not target_root:
            if init_target_tree and category_region_map:
                anchor_roots: List[str] = []
                seen_anchors = set()
                for r in organize_roots:
                    anchor = find_top_anchor_root(r, organize_roots)
                    if anchor and anchor not in seen_anchors:
                        seen_anchors.add(anchor)
                        anchor_roots.append(anchor)
                for organize_root in anchor_roots:
                    ensure_organize_tree(client, organize_root, category_region_map, args.dry_run, log)
        else:
            if init_target_tree and category_region_map:
                ensure_organize_tree(client, target_root, category_region_map, args.dry_run, log)
            if scan_exclude_target:
                excluded_root_set.add(target_root)
            else:
                if target_root not in roots:
                    roots = roots + [target_root]
        roots = [r for r in roots if norm_path(r) not in excluded_root_set]
    elif excluded_root_set:
        roots = [r for r in roots if norm_path(r) not in excluded_root_set]

    if not roots:
        print(
            "No library roots after exclusions. Provide --roots (comma-separated) or enable --auto-roots, and verify excluded roots.",
            file=sys.stderr,
        )
        sys.exit(2)

    # 3) Fast utilities: discover-only / search-only
    if args.discover_only:
        print(",".join(roots))
        return
    if args.search_only:
        keyword = args.search_only.strip()
        hits = search_series_dirs(client=client, roots=roots, keyword=keyword, skip_dir_regex=args.skip_dir_regex)
        for h in hits:
            if norm_path(h) not in excluded_root_set:
                print(h)
        return

    # 4) Rename mode requires TMDB
    tmdb_key = (args.tmdb_key or "").strip()
    placeholder_keys = {"your_tmdb_key", "tmdb_key", "your-key", "your_key", "changeme"}
    if (not tmdb_key) or (tmdb_key.lower() in placeholder_keys):
        print(
            "TMDB key is not configured. Please open WebUI settings and fill a real TMDB API key (not placeholder text like 'your_tmdb_key').",
            file=sys.stderr,
        )
        sys.exit(2)
    args.tmdb_key = tmdb_key

    tmdb = TMDBClient(api_key=args.tmdb_key, language=args.tmdb_lang, sleep=args.tmdb_sleep)

    ai: Optional[AIClient] = None
    if (not args.no_ai) and args.ai_api_key:
        ai = AIClient(
            base_url=args.ai_base_url,
            api_key=args.ai_api_key,
            model=args.ai_model,
            sleep=args.ai_sleep,
            verify_tls=(not args.insecure),
        )

    # 5) Build target series list
    keyword = (args.keyword or "").strip()
    series_paths: List[str] = []
    hub.emit('INFO', f"[SCAN] building candidate series list roots={roots} keyword={keyword or '-'} max_depth=4")

    if keyword:
        hub.emit('INFO', f"[SCAN] keyword search started: {keyword}")
        series_paths = [
            p for p in pick_series_dirs(
                client=client,
                roots=roots,
                keyword=keyword,
                skip_dir_regex=args.skip_dir_regex,
                ai=ai,
            )
            if norm_path(p) not in excluded_root_set
        ]
        if not series_paths:
            print(f"No matching series folder for keyword: {keyword}", file=sys.stderr)
            sys.exit(1)
    else:
        def collect_series_dirs(start_path: str, max_depth: int = 4) -> List[str]:
            collected: List[str] = []
            queue: List[Tuple[str, int]] = [(norm_path(start_path), 0)]
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
                    if isinstance(d, str):
                        name = d.strip()
                        full = join_path(cur, name) if name else ""
                    elif isinstance(d, dict):
                        name = str(d.get("name") or "").strip()
                        raw_path = str(d.get("path") or "").strip()
                        full = norm_path(raw_path) if raw_path else (join_path(cur, name) if name else "")
                    else:
                        name = str(getattr(d, "name", "") or "").strip()
                        full = join_path(cur, name) if name else ""
                    if not name or not full:
                        continue
                    full_norm = norm_path(full)
                    if full_norm in excluded_root_set or should_skip_misc_folder(name, args.skip_dir_regex):
                        continue
                    if full_norm not in series_paths:
                        series_paths.append(full_norm)
                    if depth < max_depth:
                        queue.append((full_norm, depth + 1))
            return collected

        for r in roots:
            if norm_path(r) in excluded_root_set:
                hub.emit('INFO', f"[SCAN] skip excluded root: {r}")
                continue
            before_count = len(series_paths)
            hub.emit('INFO', f"[SCAN] scanning root: {r}")
            collect_series_dirs(r)
            hub.emit('INFO', f"[SCAN] root done: {r} added={len(series_paths) - before_count} total={len(series_paths)}")
        if args.max_series and args.max_series > 0:
            series_paths = series_paths[: args.max_series]
            hub.emit('INFO', f"[SCAN] max_series applied: {args.max_series}")

    hub.emit('INFO', f"[SCAN] candidate series folders: {len(series_paths)}")

    state_file = (args.state_file or '').strip()
    if args.resume and not state_file:
        state_file = os.path.join(cfg_logs, 'state.jsonl')
    done_set = load_state(state_file) if (args.resume and state_file) else set()
    if done_set:
        hub.emit('INFO', f"[RESUME] loaded {len(done_set)} done series from: {state_file}")

    undo_logger = None
    if not args.dry_run:
        undo_path = (args.undo_log or '').strip()
        if not undo_path:
            undo_path = os.path.join(cfg_logs, f'undo-{ts}.jsonl')
        os.makedirs(os.path.dirname(undo_path) or '.', exist_ok=True)
        undo_logger = UndoLogger(undo_path)
        hub.emit('INFO', f"[UNDO] recording to: {undo_path}")

    hub.running = True

    for series_path in series_paths:
        # Graceful stop (triggered via web UI /api/stop)
        if hasattr(log, "hub") and getattr(getattr(log, "hub", None), "stop_requested", None):
            if log.hub.stop_requested():
                log.append("[STOP] apply stopped by user request")
                break
        sp_norm = norm_path(series_path)
        if sp_norm in done_set:
            log.append(f"[SKIP] resume already done: {series_path}")
            continue
        log.append(f"\n=== PROCESS: {series_path} ===")
        try:
            final_series_path, meta = process_series_folder(
                client=client,
                tmdb=tmdb,
                ai=ai,
                series_path=series_path,
                season_fmt=args.season_format,
                rename_series=(not args.no_rename_series),
                rename_files=(not args.no_rename_files),
                fix_bare_sxxeyy=args.fix_bare_sxxeyy,
                dry_run=args.dry_run,
                cache=cache,
                log=log,
                skip_dir_regex=args.skip_dir_regex,
                undo=undo_logger,
                library_roots=roots,
                depth=0,
            )
            if organize_enabled:
                organize_root = target_root
                if not organize_root:
                    organize_root = find_top_anchor_root(final_series_path, roots) or find_top_anchor_root(series_path, roots) or ""
                if organize_root:
                    meta_category, meta_region = (getattr(meta, "category", None), getattr(meta, "region", None))
                    move_meta = SeriesMeta(
                        tv_id=getattr(meta, "tv_id", 0),
                        name=getattr(meta, "name", ""),
                        year=getattr(meta, "year", None),
                        season_hint=getattr(meta, "season_hint", None),
                        category=meta_category,
                        region=meta_region,
                        media_type=getattr(meta, "media_type", None),
                        source_language=getattr(meta, "source_language", None),
                        keywords=getattr(meta, "keywords", None),
                        ai_inferred=bool(getattr(meta, "ai_inferred", False)),
                        tmdb_confident=bool(getattr(meta, "tmdb_confident", True)),
                    )
                    dst_dir = pick_organized_destination(final_series_path, organize_root, category_region_map, move_meta)
                    final_series_path = maybe_move_folder_to_dir(client, final_series_path, dst_dir, args.dry_run, log, undo=undo_logger)
            append_state(state_file, {"series_path": norm_path(final_series_path), "status": "done", "ts": now_ts()})
        except Exception as ex:
            log.append(f"[ERROR] {series_path}: {ex}")
            append_state(state_file, {"series_path": sp_norm, "status": "error", "error": str(ex), "ts": now_ts()})

    hub.running = False

    try:
        with open(args.cache, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    hub.emit("INFO", f"[DONE] Log saved: {log_file}")
    hub.close()


def main(argv=None):
    cfg_dir = get_config_dir()
    store = RuntimeConfigStore(cfg_dir)
    base_config = store.load()
    apply_runtime_config(base_config)

    ap = build_runtime_parser()
    args = ap.parse_args(argv)

    merged_config = store.merge_cli_overrides(base_config, args)
    runtime_args = ap.parse_args(store.config_to_argv(merged_config))

    if getattr(args, 'save_config', False):
        store.save(merged_config)
        apply_runtime_config(merged_config)
    else:
        apply_runtime_config(merged_config)

    return run_job(runtime_args, cfg_dir=cfg_dir, store=store, start_webui=True)


def run_webui():
    cfg_dir = get_config_dir()
    store = RuntimeConfigStore(cfg_dir)
    apply_runtime_config(store.load())

    hub = LogHub(log_file='', also_print=False, keep=500)
    hub.running = False
    token = store.get_admin_password()
    srv = start_log_server(hub, host='0.0.0.0', port=55255, token=(token or None))

    host = store.get_public_host()
    query = urlencode({'token': token}) if token else ''
    suffix = f'?{query}' if query else ''
    public_url = f"{host.rstrip('/')}{suffix}" if host else f"http://127.0.0.1:{int(srv.port)}{suffix}"

    hub.emit('INFO', '[WEBUI] config center ready')
    hub.emit('INFO', f'[WEBUI] open: {public_url}')

    stop_event = threading.Event()
    runtime_state = {
        'running': False,
        'state': 'idle',
        'last_error': '',
        'started_at': '',
        'stopped_at': '',
        'last_start_time': '',
        'last_stop_time': '',
        'argv': [],
    }
    runtime_lock = threading.Lock()

    def _now_text():
        return time.strftime('%Y-%m-%d %H:%M:%S')

    def _set_runtime(**kwargs):
        with runtime_lock:
            runtime_state.update(kwargs)

    def _get_runtime():
        with runtime_lock:
            data = dict(runtime_state)
        data['stop_requested'] = stop_event.is_set()
        return data

    def _reload(_payload=None):
        try:
            cfg = store.load()
            apply_runtime_config(cfg)
            hub.emit('INFO', '[WEBUI] config reloaded from disk')
            return {'ok': True, 'reloaded': True}
        except Exception as ex:
            hub.emit('ERROR', f'[WEBUI] reload failed: {ex}')
            raise

    def _run_with_config(cfg):
        _set_runtime(
            running=True,
            state='running',
            last_error='',
            started_at=_now_text(),
            last_start_time=_now_text(),
            stopped_at='',
            last_stop_time='',
            argv=store.config_to_argv(cfg),
        )
        hub.running = True
        try:
            runtime_args = build_runtime_parser().parse_args(store.config_to_argv(cfg))
            run_job(runtime_args, cfg_dir=cfg_dir, store=store, start_webui=False, hub=hub)
            _set_runtime(
                running=False,
                state='idle',
                stopped_at=_now_text(),
                last_stop_time=_now_text(),
            )
        except SystemExit as ex:
            code = ex.code if isinstance(ex.code, int) else 1
            _set_runtime(
                running=False,
                state='idle' if code == 0 else 'error',
                last_error='' if code == 0 else f'SystemExit: {ex.code}',
                stopped_at=_now_text(),
                last_stop_time=_now_text(),
            )
            if code:
                hub.emit('ERROR', f'[WEBUI] task exited: {ex.code}')
        except Exception as ex:
            _set_runtime(
                running=False,
                state='error',
                last_error=str(ex),
                stopped_at=_now_text(),
                last_stop_time=_now_text(),
            )
            hub.emit('ERROR', f'[WEBUI] task failed: {ex}')
        finally:
            hub.running = False

    def _on_run(payload):
        cfg = store.save(payload or {})
        apply_runtime_config(cfg)
        scan_roots = list(cfg.get('scan_roots') or [])
        logger.info("[WEBUI] start requested dry_run=%s scan_roots=%s target_root=%s auto_discover=%s", bool(cfg.get('dry_run', True)), scan_roots, cfg.get('target_root') or '', bool(cfg.get('auto_discover')))
        hub.emit('INFO', '[WEBUI] config saved; task starting')
        worker = threading.Thread(target=_run_with_config, args=(cfg,), daemon=True)
        worker.start()
        logger.info("[WEBUI] worker thread started ident=%s name=%s", worker.ident, worker.name)
        return {'ok': True, 'argv': store.config_to_argv(cfg)}

    if hasattr(hub, 'set_runtime_hooks'):
        try:
            hub.set_runtime_hooks(on_run=_on_run, on_reload=_reload, stop_event=stop_event, config_store=store, runtime_getter=_get_runtime)
        except Exception as ex:
            hub.emit('ERROR', f'[WEBUI] hook setup failed: {ex}')

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        srv.stop()
        hub.close()


if __name__ == "__main__":
    cli_mode = bool(CURRENT_RUNTIME_CONFIG.get('cli_mode', False))
    if cli_mode or len(sys.argv) > 1:
        main()
    else:
        run_webui()
