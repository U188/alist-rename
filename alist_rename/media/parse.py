"""Media parsing helpers split from legacy renamer."""
from __future__ import annotations

import os
import re
from datetime import date
from typing import Dict, List, Optional, Tuple

from alist_rename.common.text import chinese_to_int, normalize_spaces, to_halfwidth
from alist_rename.common.text import normalize_title_for_compare

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".m4v", ".ts", ".m2ts", ".webm"}
SXXEYY_RE = re.compile(r"(?i)S(\d{1,2})\s*E(\d{1,3})")

_EP_NUM_RE = re.compile(r"(?i)\b(?:EP|E)(\d{1,3})\b")

_1X02_RE = re.compile(r"(?i)\b(\d{1,2})\s*[xX]\s*(\d{1,3})\b")

_CN_EP_RE = re.compile(r"第\s*([一二三四五六七八九十\d]{1,4})\s*(?:集|话|回)")

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

_DATE8_RE = re.compile(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")

_DATE_SEP_RE = re.compile(r"(?<!\d)(20\d{2})[.\-_](\d{1,2})[.\-_](\d{1,2})(?!\d)")

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

_RES_RE = re.compile(r"(?i)\b(4320|2160|1440|1080|720|576|540|480)p\b")

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

def is_season_dir(name: str) -> bool:
    return parse_season_from_text(name) is not None

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

    return season_hint, None, False, " ".join(_quality_tokens(stem2))

def is_special_episode_name(name: str) -> bool:
    n = to_halfwidth(name or "").lower()
    for m in _SPECIAL_MARKERS:
        if m.lower() in n:
            return True
    return False

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

__all__ = [name for name in globals() if not name.startswith("__")]
