#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compatibility shim for the pre-refactor monolithic renamer module."""
from __future__ import annotations

from alist_rename.cli import _start_logui_if_needed, build_runtime_parser, run_job, main, run_webui

__all__ = ["_start_logui_if_needed", "build_runtime_parser", "run_job", "main", "run_webui"]
