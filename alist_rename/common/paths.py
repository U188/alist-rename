"""Path and timestamp helpers."""
import os
from datetime import datetime
from typing import Tuple

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
    """Resolve config directory from the project root (where config.json lives)."""
    # This module is alist_rename/common/paths.py; config.json is kept beside
    # renamer.py/logui.py at the repository/runtime root, matching the legacy app.
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

__all__ = ["norm_path", "join_path", "split_path", "now_ts", "get_config_dir"]
