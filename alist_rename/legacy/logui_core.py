#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compatibility shim for the pre-refactor WebUI/log module."""
from __future__ import annotations

from alist_rename.web.hub import LogEvent, LogHub
from alist_rename.web.handler import make_handler
from alist_rename.web.live_log import LiveLog, start_log_server

__all__ = ["LogEvent", "LogHub", "make_handler", "LiveLog", "start_log_server"]
