#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Command line and WebUI runtime entrypoints for alist-rename."""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import threading
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from logui import LogHub, LiveLog, start_log_server
from runtime_config import RuntimeConfigStore, apply_runtime_config, CURRENT_RUNTIME_CONFIG

from alist_rename.common.paths import get_config_dir, join_path, norm_path, now_ts
from alist_rename.media.models import SeriesMeta
from alist_rename.clients.alist import AlistClient
from alist_rename.clients.tmdb import TMDBClient
from alist_rename.clients.ai import AIClient
from alist_rename.ops.state import UndoLogger, load_state, append_state
from alist_rename.ops.undo import apply_undo
from alist_rename.ops.filesystem import maybe_move_folder_to_dir
from alist_rename.ops.cleanup import parse_csv_paths, remove_empty_source_dirs, remove_empty_target_root_dirs, should_skip_misc_folder
from alist_rename.media.resolver import (
    ensure_organize_tree, parse_boolish, parse_category_region_map, pick_organized_destination,
)
from alist_rename.scanner.discover import (
    discover_library_roots, find_top_anchor_root, pick_series_dirs, search_series_dirs,
)
from alist_rename.scanner.processor import process_series_folder

_EXACT_SEASON_DIR_RE = re.compile(
    r"(?ix)^\s*(?:"
    r"S\d{1,2}"
    r"|Season\s*\d{1,2}"
    r"|第\s*[一二三四五六七八九十\d]+\s*季"
    r"|\d{1,2}\s*季"
    r")\s*$"
)


def is_exact_season_dir_name(name: str) -> bool:
    """Return True only for pure season folders like S01/Season 1/第1季."""
    return bool(_EXACT_SEASON_DIR_RE.match(str(name or "")))

logger = logging.getLogger("embyrename")
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
    ap.add_argument("--no-skip-exact-duplicate-files", dest="skip_exact_duplicate_files", action="store_false", default=True, help="When moving same-name files, do not skip files that have the same size/hash as destination")
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
                if not roots:
                    target_parts = [x for x in target_root.split("/") if x]
                    roots = ["/" + target_parts[0]] if target_parts else ["/"]
                elif target_root not in roots:
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
        organize_root_norm = norm_path(target_root or "") if organize_enabled and target_root else ""
        def _bucket_names(value: Any) -> set:
            if isinstance(value, (list, tuple, set)):
                raw_items = value
            else:
                raw_items = parse_csv_paths(str(value or ""))
            return {str(v).strip().strip('/') for v in raw_items if str(v).strip().strip('/')}

        configured_category_names = _bucket_names(getattr(args, "category_buckets", ""))
        mapped_category_names = {str(k).strip().strip('/') for k in (category_region_map or {}).keys() if str(k).strip().strip('/')}
        category_names = configured_category_names or mapped_category_names
        mapped_region_names = {str(v).strip().strip('/') for vals in (category_region_map or {}).values() for v in (vals or []) if str(v).strip().strip('/')}
        configured_region_names = _bucket_names(getattr(args, "region_buckets", ""))
        region_names = mapped_region_names or configured_region_names

        def _cleanup_bucket_names(value: Any) -> List[str]:
            if isinstance(value, (list, tuple, set)):
                raw_items = value
            else:
                raw_items = parse_csv_paths(str(value or ""))
            out: List[str] = []
            for item in raw_items:
                name = str(item or "").strip().strip('/')
                if name and name not in out:
                    out.append(name)
            return out

        protected_category_names = _cleanup_bucket_names(getattr(args, "category_buckets", ""))
        if not protected_category_names:
            protected_category_names = [
                str(k or "").strip().strip('/')
                for k in (category_region_map or {}).keys()
                if str(k or "").strip().strip('/')
            ]

        def cleanup_target_root_empty_dirs_now(reason: str):
            if not (organize_enabled and target_root):
                return
            try:
                hub.emit("INFO", f"[EMPTY] target-root cleanup before {reason}: {target_root}")
                remove_empty_target_root_dirs(
                    client=client,
                    target_root=target_root,
                    protected_names=protected_category_names,
                    hub=hub,
                    dry_run=args.dry_run,
                )
            except Exception as e:
                hub.emit("WARN", f"[EMPTY] target-root cleanup skipped: {e}")

        def is_organize_container_path(path: str) -> bool:
            """Skip configured organize-root/category/region container dirs.

            When scan roots are auto-derived from the target root's parent (for
            example target_root=/天翼/影视 -> scan root=/天翼), the target root
            itself is only a library container, not a series folder.  Category
            and region buckets below it are also containers; real series folders
            under those buckets must still be traversed and collected so wrongly
            classified items can be re-resolved and moved to the TMDB/AI-derived
            destination.
            """
            path_norm = norm_path(path)
            if not organize_root_norm:
                return False
            if path_norm == organize_root_norm:
                return True
            prefix = organize_root_norm.rstrip('/') + '/'
            if not path_norm.startswith(prefix):
                return False
            rel = path_norm[len(prefix):].strip('/')
            if not rel:
                return True
            parts = [p for p in rel.split('/') if p]
            if len(parts) == 1:
                return parts[0] in category_names
            if len(parts) == 2:
                return parts[0] in category_names and parts[1] in region_names
            return False

        def collect_series_dirs(start_path: str, max_depth: int = 6) -> List[str]:
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
                    is_container = is_organize_container_path(full_norm)
                    if is_container:
                        hub.emit('INFO', f"[SCAN] skip organize container: {full_norm}")
                    elif is_exact_season_dir_name(name):
                        hub.emit('INFO', f"[SCAN] skip season folder candidate: {full_norm}")
                    elif full_norm not in series_paths:
                        series_paths.append(full_norm)
                    if depth < max_depth:
                        queue.append((full_norm, depth + 1))
            return collected

        for r in roots:
            if norm_path(r) in excluded_root_set:
                hub.emit('INFO', f"[SCAN] skip excluded root: {r}")
                continue
            if organize_root_norm and norm_path(r) == organize_root_norm:
                cleanup_target_root_empty_dirs_now("scan")
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
        _sp_parent, _sp_name = os.path.split(sp_norm.rstrip('/'))
        if is_exact_season_dir_name(_sp_name):
            log.append(f"[SKIP] season folder is not a series candidate: {series_path}")
            append_state(state_file, {"series_path": sp_norm, "status": "skipped_season_dir", "ts": now_ts()})
            continue
        if sp_norm in done_set:
            log.append(f"[SKIP] resume already done: {series_path}")
            continue
        log.append(f"\n=== PROCESS: {series_path} ===")
        try:
            result = process_series_folder(
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
                organize_root=(target_root or ""),
                category_region_map=category_region_map,
            )
            if not isinstance(result, tuple) or len(result) != 2:
                log.append(f"[WARN] process returned invalid result for {series_path}: {result!r}; skip this item")
                append_state(state_file, {"series_path": sp_norm, "status": "error", "error": f"invalid process result: {result!r}", "ts": now_ts()})
                continue
            final_series_path, meta = result
            if hasattr(log, "hub") and getattr(getattr(log, "hub", None), "stop_requested", None):
                if log.hub.stop_requested():
                    log.append(f"[STOP] interrupted during series; not marking done: {series_path}")
                    append_state(state_file, {"series_path": sp_norm, "status": "stopped", "ts": now_ts()})
                    break
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
            tb = traceback.format_exc()
            log.append(f"[ERROR] {series_path}: {ex}\n{tb}")
            append_state(state_file, {"series_path": sp_norm, "status": "error", "error": str(ex), "ts": now_ts()})

    if organize_enabled and target_root:
        try:
            def _cleanup_bucket_names(value: Any) -> List[str]:
                if isinstance(value, (list, tuple, set)):
                    raw_items = value
                else:
                    raw_items = parse_csv_paths(str(value or ""))
                out: List[str] = []
                for item in raw_items:
                    name = str(item or "").strip().strip('/')
                    if name and name not in out:
                        out.append(name)
                return out

            protected_category_names = _cleanup_bucket_names(getattr(args, "category_buckets", ""))
            if not protected_category_names:
                protected_category_names = [
                    str(k or "").strip().strip('/')
                    for k in (category_region_map or {}).keys()
                    if str(k or "").strip().strip('/')
                ]
            remove_empty_target_root_dirs(
                client=client,
                target_root=target_root,
                protected_names=protected_category_names,
                hub=hub,
                dry_run=args.dry_run,
            )
        except Exception as e:
            hub.emit("WARN", f"[EMPTY] target-root cleanup skipped: {e}")

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
    runtime_argv = store.config_to_argv(merged_config)
    # Preserve utility/transient CLI flags that are intentionally not persisted
    # in config.json. Without this, commands like `--search-only` or `--undo`
    # are parsed once, lost during config->argv rebuild, and unexpectedly fall
    # through into a full rename run.
    if getattr(args, 'discover_only', False):
        runtime_argv.append('--discover-only')
    if getattr(args, 'search_only', ''):
        runtime_argv.extend(['--search-only', str(args.search_only)])
    if getattr(args, 'undo', ''):
        runtime_argv.extend(['--undo', str(args.undo)])
    if getattr(args, 'yes', False):
        runtime_argv.append('--yes')
    runtime_args = ap.parse_args(runtime_argv)

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
        start_text = _now_text()
        run_stamp = time.strftime('%Y%m%d-%H%M%S')
        log_dir = os.path.join(cfg_dir, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        run_log = os.path.join(log_dir, f'webui-run-{run_stamp}.log')
        latest_log = os.path.join(log_dir, 'latest-webui.log')
        try:
            if hasattr(hub, 'set_log_file'):
                hub.set_log_file(run_log)
        except Exception as ex:
            hub.emit('ERROR', f'[WEBUI] attach run log failed: {ex}')
        try:
            with open(latest_log, 'w', encoding='utf-8') as fh:
                fh.write(f'# latest WebUI run log\n# run_log={run_log}\n# started_at={start_text}\n')
        except Exception:
            pass
        try:
            if hasattr(hub, 'set_latest_log_file'):
                hub.set_latest_log_file(latest_log)
        except Exception:
            pass
        _set_runtime(
            running=True,
            state='running',
            last_error='',
            started_at=start_text,
            last_start_time=start_text,
            stopped_at='',
            last_stop_time='',
            argv=store.config_to_argv(cfg),
            log_file=run_log,
            latest_log_file=latest_log,
        )
        hub.running = True
        hub.emit('INFO', f'[WEBUI] persistent log: {run_log}')
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
        # A previous /api/stop leaves stop_event set.  Clear it before
        # launching a new worker, otherwise the next run exits immediately
        # with "[STOP] ... stopped by user request" after the scan phase.
        try:
            stop_event.clear()
        except Exception:
            pass
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
