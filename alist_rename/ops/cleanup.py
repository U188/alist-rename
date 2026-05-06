"""Cleanup and subtitle relocation helpers."""
from __future__ import annotations

import logging
import os
import re
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from logui import LogHub
from alist_rename.config import CURRENT_RUNTIME_CONFIG
from alist_rename.clients.alist import AlistClient
from alist_rename.common.paths import join_path, norm_path
from alist_rename.common.text import bool_env
from alist_rename.media.parse import is_season_dir, parse_episode_from_name
from alist_rename.media.naming import season_folder_name
from alist_rename.ops.filesystem import maybe_move

SUB_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub"}
log = logging.getLogger(__name__)

DEFAULT_SKIP_DIR_REGEX = r"(福利|广告|推广|促销|活动|限时福利|限时|UC官方|阿里|Promo|sample|Samples?|Extras?|花絮|特典|周边|海报|Poster|封面|截图|Thumbs|@eaDir|\\.sync|lost\\+found)"



def logger(hub: Optional["LogHub"], level: str, message: str):
    if hub:
        hub.emit(level, message)
    else:
        print(f"{level}: {message}")

MISC_DIR_NAMES = {
    "@eadir", "__macosx", ".ds_store",
    "sample", "samples", "screens", "screen", "screenshots",
    "extras", "extra", "bonus", "bts",
    "poster", "posters", "fanart", "thumb", "thumbs", "artwork",
    "cd1", "cd2",
    "subs", "sub", "subtitle", "subtitles", "字幕", "字幕组",
}

SUBTITLE_DIR_NAMES = {"subs", "sub", "subtitle", "subtitles", "字幕", "字幕组", "subtitles&subs"}

SUBTITLE_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx"}

AD_DELETE_EXTS = {
    ".url", ".lnk", ".html", ".htm",
    ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
}

JUNK_MARKERS = [
    "防走丢", "更多资源", "公众号", "关注", "扫码", "加群", "群号", "最新地址",
    "备用网址", "网址", "www.", "http://", "https://", "telegram", "t.me", "qq群", "qq群号",
]

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

def cleanup_ads_in_dir(client: AlistClient, dir_path: str, hub: Optional["LogHub"], dry_run: bool = False):
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
        log.warning("[WARN] cleanup listdir failed: %s : %s", dir_path, e)
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

def _emit_cleanup(hub: Optional["LogHub"], level: str, msg: str):
    if hub:
        try:
            hub.emit(level, msg)
            return
        except Exception:
            pass
    if level.upper() in {"WARN", "ERROR"}:
        log.warning(msg)
    else:
        log.info(msg)

def report_empty_dir(client: AlistClient, dir_path: str, hub: Optional["LogHub"], dry_run: bool = False):
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
    _emit_cleanup(hub, "WARN", msg)

def remove_empty_source_dirs(
    client: AlistClient,
    dir_paths: List[str],
    hub: Optional["LogHub"],
    dry_run: bool = False,
    skip_dir_regex: str = "",
):
    """Optionally remove ordinary source subdirectories that became empty in this run.

    Safety constraints:
    - Disabled by default via delete_empty_source_dirs.
    - Only paths explicitly collected from this run are considered.
    - Never removes root, season dirs, skipped/junk dirs, subtitle dirs, or non-empty dirs.
    """
    if not bool(CURRENT_RUNTIME_CONFIG.get("delete_empty_source_dirs", False)):
        return

    seen = set()
    for raw in dir_paths or []:
        dir_path = norm_path(raw)
        if not dir_path or dir_path in {"/", "."} or dir_path in seen:
            continue
        seen.add(dir_path)
        name = os.path.basename(dir_path.rstrip("/"))
        if not name:
            continue
        if is_season_dir(name) or is_subtitle_dir_name(name) or should_skip_misc_folder(name, skip_dir_regex):
            continue
        try:
            entries = client.list_dir(dir_path, refresh=False)
        except Exception as e:
            _emit_cleanup(hub, "WARN", f"[EMPTY] check failed: {dir_path} ({e})")
            continue
        if entries:
            continue
        if dry_run:
            _emit_cleanup(hub, "WARN", f"[DRY] [EMPTY] would remove empty source folder: {dir_path}")
            continue
        parent, child = os.path.split(dir_path.rstrip("/"))
        parent = norm_path(parent or "/")
        if not child:
            continue
        try:
            client.remove(parent, [child])
            _emit_cleanup(hub, "INFO", f"[EMPTY] removed empty source folder: {dir_path}")
        except Exception as e:
            _emit_cleanup(hub, "WARN", f"[EMPTY] remove failed: {dir_path} ({e})")


def remove_empty_target_root_dirs(
    client: AlistClient,
    target_root: str,
    protected_names: List[str],
    hub: Optional["LogHub"],
    dry_run: bool = False,
):
    """Remove empty direct children under target_root, except configured category containers.

    This is intentionally narrow: it only checks target_root/* and never descends.
    Configured 一级分类容器 (for example 动漫/电视剧/电影 or user-custom buckets)
    are protected even when empty; accidental empty folders such as "80动漫（1983）"
    can be cleaned when delete_empty_source_dirs is enabled.
    """
    if not bool(CURRENT_RUNTIME_CONFIG.get("delete_empty_source_dirs", False)):
        return

    root = norm_path(target_root or "")
    if not root or root in {"/", "."}:
        return
    protected = {str(n or "").strip().strip("/") for n in (protected_names or []) if str(n or "").strip().strip("/")}

    try:
        entries = client.list_dir(root, refresh=False)
    except Exception as e:
        _emit_cleanup(hub, "WARN", f"[EMPTY] target root check failed: {root} ({e})")
        return

    for ent in entries or []:
        try:
            is_dir = bool(getattr(ent, "is_dir", False))
            name = str(getattr(ent, "name", "") or "").strip()
        except Exception:
            continue
        if not is_dir or not name or name in protected:
            continue
        child_path = norm_path(f"{root.rstrip('/')}/{name}")
        try:
            child_entries = client.list_dir(child_path, refresh=False)
        except Exception as e:
            _emit_cleanup(hub, "WARN", f"[EMPTY] target child check failed: {child_path} ({e})")
            continue
        if child_entries:
            continue
        if dry_run:
            _emit_cleanup(hub, "WARN", f"[DRY] [EMPTY] would remove empty target-root folder: {child_path}")
            continue
        try:
            client.remove(root, [name])
            _emit_cleanup(hub, "INFO", f"[EMPTY] removed empty target-root folder: {child_path}")
        except Exception as e:
            _emit_cleanup(hub, "WARN", f"[EMPTY] remove target-root folder failed: {child_path} ({e})")

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

__all__ = [name for name in globals() if not name.startswith("__")]
