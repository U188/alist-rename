#!/usr/bin/env python3
"""Backward-compatible CLI entry for alist-rename."""
import sys

from alist_rename.cli import *  # noqa: F401,F403
from alist_rename.cli import main, run_webui
from alist_rename.config import CURRENT_RUNTIME_CONFIG

if __name__ == "__main__":
    cli_mode = bool(CURRENT_RUNTIME_CONFIG.get('cli_mode', False))
    if cli_mode or len(sys.argv) > 1:
        main()
    else:
        run_webui()
