"""Series folder processing pipeline."""
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

from alist_rename.clients.ai import AIClient
from alist_rename.clients.alist import AlistClient
from alist_rename.clients.tmdb import TMDBClient
from alist_rename.common.paths import join_path, norm_path, split_path
from alist_rename.common.text import safe_filename
from alist_rename.config import CURRENT_RUNTIME_CONFIG
from alist_rename.media.models import SeriesMeta
from alist_rename.media.naming import (
    build_new_sidecar_name, build_new_video_name, build_prefixed_sxxeyy_name, season_folder_name,
)
from alist_rename.media.parse import (
    needs_series_prefix_for_sxxeyy, parse_episode_from_name, parse_season_from_text,
)
from alist_rename.media.resolver import (
    CATEGORY_CONTAINER_NAMES, VIDEO_EXTS, gather_series_context, infer_variety_and_special_episodes,
    is_same_show_container_folder, looks_like_show_folder_name, pick_organized_destination, resolve_series,
)
from alist_rename.ops.cleanup import (
    cleanup_ads_in_dir, contains_junk_marker, relocate_subtitles_in_show_root, report_empty_dir,
    should_skip_misc_folder,
)
from alist_rename.ops.filesystem import (
    ensure_dir, maybe_move, maybe_move_folder_to_dir, maybe_rename, maybe_rename_path, related_sidecars,
)
from alist_rename.ops.state import UndoLogger
from alist_rename.scanner.discover import find_library_root, is_season_container_folder

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
    organize_root: str = "",
    category_region_map: Optional[Dict[str, List[str]]] = None,
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
                organize_root=organize_root,
                category_region_map=category_region_map,
            )
            processed_child, _child_meta = child_result if isinstance(child_result, tuple) else (child_path, None)
            try:
                if organize_root and _child_meta:
                    dst_dir = pick_organized_destination(processed_child, organize_root, category_region_map or {}, _child_meta)
                else:
                    dst_dir = lib_root
                maybe_move_folder_to_dir(client, processed_child, dst_dir, dry_run, log, undo=undo)
            except Exception as e:
                log.append(f"[ERROR] move nested show to target failed: {processed_child} -> {dst_dir if 'dst_dir' in locals() else lib_root} ({e})")
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
                    organize_root=organize_root,
                    category_region_map=category_region_map,
                )
                child_final, child_meta = nested_result if isinstance(nested_result, tuple) else (child_path, None)
                if organize_root and child_meta:
                    child_dst_dir = pick_organized_destination(child_final, organize_root, category_region_map or {}, child_meta)
                else:
                    child_dst_dir = lib_root
                _ = maybe_move_folder_to_dir(client, child_final, child_dst_dir, dry_run, log, undo=undo)
            except Exception as e:
                log.append(f"[ERROR] failed to process nested show folder {child_path}: {e}")

    report_empty_dir(client, new_series_path, hub, dry_run=dry_run)
    return new_series_path, meta

__all__ = ["process_series_folder"]
