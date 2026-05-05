"""Front-end template loader for the WebUI."""
from __future__ import annotations

from pathlib import Path

_TEMPLATE_DIR = Path(__file__).with_suffix("")
_INDEX_HTML = (_TEMPLATE_DIR / "index.html").read_text(encoding="utf-8")

__all__ = ["_INDEX_HTML"]
