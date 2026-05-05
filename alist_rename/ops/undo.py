"""Undo operation runner."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

from alist_rename.clients.alist import AlistClient

if TYPE_CHECKING:
    from logui import LogHub
from alist_rename.common.paths import join_path

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

__all__ = ["apply_undo"]
