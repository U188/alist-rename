"""Text normalization and filename helpers."""
import re
from typing import Optional
from alist_rename.config import CURRENT_RUNTIME_CONFIG

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

__all__ = ["normalize_spaces", "to_halfwidth", "bool_env", "normalize_quality_tail", "safe_filename", "clean_series_query", "chinese_to_int", "extract_year_hint", "normalize_title_for_compare"]
