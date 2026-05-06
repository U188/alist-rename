"""AList filesystem operation helpers."""
from __future__ import annotations

import os
import re
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from alist_rename.ops.state import UndoLogger

from runtime_config import CURRENT_RUNTIME_CONFIG
from alist_rename.clients.alist import AlistClient
from alist_rename.common.paths import join_path, norm_path, now_ts, split_path
from alist_rename.common.text import safe_filename
from alist_rename.media.models import DirEntry

SUB_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub"}

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

def _same_file_identity(a: DirEntry | None, b: DirEntry | None) -> bool:
    """Best-effort exact duplicate check for same-name files."""
    if not a or not b or a.is_dir or b.is_dir:
        return False
    if a.name != b.name:
        return False
    if a.size is not None and b.size is not None and a.size == b.size:
        if a.hash_info and b.hash_info:
            return a.hash_info == b.hash_info
        return True
    return False

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

def path_is_dir(client: AlistClient, path: str) -> bool:
    """Return True only when an AList directory already exists.

    This intentionally performs a read/list probe instead of creating anything.
    It is used by move operations to avoid accidentally materializing guessed
    organize destinations (for example many category/region folders).
    """
    path = norm_path(path)
    if path in {"", "/"}:
        return True
    try:
        client.list_dir(path, refresh=False)
        return True
    except Exception:
        return False

def ensure_path(client: AlistClient, path: str, dry_run: bool, log: List[str]) -> str:
    """Ensure a possibly multi-level directory path exists."""
    path = norm_path(path)
    if path in {"", "/"}:
        return path or "/"

    parts = [p for p in path.strip("/").split("/") if p]
    current = "/"
    for part in parts:
        current = ensure_dir(client, current, part, dry_run, log, assume_exists=dry_run)
    return current

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

    if dry_run:
        log.append(f"[DRY] rename {full_path} -> {new_name}")
        # In dry-run, previous planned moves/mkdirs may have produced virtual
        # destination directories that do not exist on AList yet.  Do not query
        # the parent for conflict detection here; just return the planned path.
        return join_path(parent, new_name) if dry_return_new else full_path

    # avoid name collision in target directory
    resolved = unique_name_in_parent(client, parent, new_name)
    if not resolved:
        log.append(f"[SKIP] conflict: {full_path} -> {new_name} (exists)")
        return full_path
    if resolved != new_name:
        log.append(f"[INFO] conflict: {new_name} exists, use {resolved}")
        new_name = resolved
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
        try:
            src_entries = client.list_dir(src_dir, refresh=False)
            src_existing = {e.name for e in src_entries}
        except Exception as ex:
            log.append(f"[SKIP] move source directory not available: {src_dir} -> {dst_dir}: {ex}")
            return
        missing = [n for n in names if n not in src_existing]
        if missing:
            log.append(f"[SKIP] move source item missing: {src_dir} names={missing} -> {dst_dir}")
        names = [n for n in names if n in src_existing]
        if not names:
            return
        log.append(f"move {names} : {src_dir} -> {dst_dir}")
        client.move(src_dir, dst_dir, names)
        if undo:
            undo.record({"op": "move", "src_dir": src_dir, "dst_dir": dst_dir, "names": names, "ts": now_ts()})
        return

    # individual moves with conflict resolution
    try:
        dst_entries = client.list_dir(dst_dir, refresh=False)
        dst_by_name = {e.name: e for e in dst_entries}
        dst_existing = set(dst_by_name)
    except Exception:
        dst_by_name = {}
        dst_existing = set()

    skip_exact_duplicate = bool(CURRENT_RUNTIME_CONFIG.get("skip_exact_duplicate_files", True))
    try:
        src_entries = client.list_dir(src_dir, refresh=False)
        src_by_name = {e.name: e for e in src_entries}
        src_list_available = True
    except Exception as ex:
        src_by_name = {}
        src_list_available = False
        log.append(f"[SKIP] move source directory not available: {src_dir} -> {dst_dir}: {ex}")

    for name in list(names):
        if not name:
            continue
        if not src_list_available:
            continue
        if name not in src_by_name:
            log.append(f"[SKIP] move source item missing: {join_path(src_dir, name)} -> {dst_dir}")
            continue
        final_name = name
        if final_name in dst_existing:
            if skip_exact_duplicate and _same_file_identity(src_by_name.get(final_name), dst_by_name.get(final_name)):
                log.append(f"[SKIP] exact duplicate already exists: {join_path(src_dir, final_name)} -> {join_path(dst_dir, final_name)}")
                continue
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
    if not path_is_dir(client, dst_dir):
        log.append(f"[SKIP] move target directory does not exist (create it first or run init organize tree): {folder_path} -> {dst_dir}")
        return folder_path

    # If destination already contains a folder with the same name, merge the
    # source folder contents into that existing folder instead of creating
    # "S01 (1)" / "Season (1)" duplicate directories.  File conflicts inside
    # the folder are still handled by maybe_move(): exact duplicate files are
    # skipped, same-name different files are renamed with suffix, different
    # versions keep coexisting.
    try:
        dst_entries = client.list_dir(dst_dir, refresh=False)
        dst_same = next((e for e in dst_entries if e.name == name), None)
    except Exception:
        dst_same = None
    if dst_same and getattr(dst_same, "is_dir", False):
        merge_dst = join_path(dst_dir, name)
        try:
            src_entries = client.list_dir(folder_path, refresh=False)
        except Exception as ex:
            log.append(f"[SKIP] merge folder failed to list source: {folder_path} -> {merge_dst}: {ex}")
            return folder_path
        child_names = [e.name for e in src_entries if e.name]
        if not child_names:
            log.append(f"[INFO] merge folder skipped empty source: {folder_path} -> {merge_dst}")
        else:
            log.append(f"[INFO] merge same-name folder: {folder_path} -> {merge_dst} ({len(child_names)} items)")
            maybe_move(client, folder_path, merge_dst, child_names, dry_run, log, undo=undo)
        from alist_rename.ops.cleanup import report_empty_dir
        report_empty_dir(client, original_folder_path, getattr(log, "hub", None), dry_run=dry_run)
        return merge_dst

    # Resolve non-directory conflict at destination
    final_name = unique_name_in_parent(client, dst_dir, name)
    if final_name != name:
        renamed = maybe_rename_path(client, folder_path, final_name, dry_run, log, undo=undo)
        folder_path = renamed
        src_parent, name = split_path(folder_path)

    maybe_move(client, src_parent, dst_dir, [name], dry_run, log, undo=undo)
    from alist_rename.ops.cleanup import report_empty_dir
    report_empty_dir(client, original_folder_path, getattr(log, "hub", None), dry_run=dry_run)
    return join_path(dst_dir, name)

__all__ = [name for name in globals() if not name.startswith("__")]
