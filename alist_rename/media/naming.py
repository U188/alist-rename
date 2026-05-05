"""Media naming helpers split from legacy renamer."""
from __future__ import annotations

import os
import re
SUB_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx", ".sup"}
SUBTITLE_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx"}

from typing import List, Optional, Tuple

from alist_rename.common.text import normalize_spaces, safe_filename, to_halfwidth
from alist_rename.media.parse import SXXEYY_RE, _extract_subtitle_lang_and_flags, extract_resolution

from alist_rename.media.parse import _quality_tokens


def normalize_quality_tail(text: str) -> str:
    if not text:
        return text
    replacements = {
        "4k": "4K", "uhd": "UHD", "hdr": "HDR", "hdr10": "HDR10",
        "hdr10+": "HDR10+", "dv": "DV", "dovi": "DV",
        "dolbyvision": "DolbyVision", "dolby vision": "DolbyVision", "dolby": "Dolby",
        "杜比视界": "DV", "杜比": "Dolby", "视界": "DV", "高码": "HiBitrate",
    }
    out = text
    for k, v in replacements.items():
        out = re.sub(re.escape(k), v, out, flags=re.I)
    out = re.sub(r"(?i)\b(\d{3,4})P\b", lambda m: m.group(1) + "p", out)
    return re.sub(r"\s+", " ", out).strip()

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

__all__ = [name for name in globals() if not name.startswith("__")]
