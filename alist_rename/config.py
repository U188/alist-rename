# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import hmac
import json
import os
import re
import secrets
from typing import Any, Dict, List

SENSITIVE_FIELDS = {
    'alist_token', 'alist_pass', 'alist_otp', 'tmdb_key', 'ai_api_key',
    'admin_password_hash', 'session_secret',
}
BOOL_FIELDS = {
    'auto_roots', 'dry_run', 'resume', 'insecure', 'no_ai',
    'organize_enabled', 'init_target_tree', 'scan_exclude_target',
    'ai_infer_season', 'protect_sxxeyy', 'log_web', 'cli_mode',
    'move_individual', 'alist_refresh', 'delete_empty_source_dirs',
    'skip_exact_duplicate_files',
}
DEFAULTS: Dict[str, Any] = {
    'alist_url': '', 'alist_token': '', 'alist_user': '', 'alist_pass': '', 'alist_otp': '',
    'tmdb_key': '', 'tmdb_lang': 'zh-CN', 'tmdb_api_base': '',
    'roots': '', 'auto_roots': False, 'discover_root_regex': '', 'discover_categories': '',
    'keyword': '', 'max_series': 0, 'season_format': 'S{:02d}',
    'rename_series': True, 'rename_files': True, 'fix_bare_sxxeyy': True,
    'dry_run': True, 'sleep': 0.2, 'tmdb_sleep': 0.25, 'skip_dir_regex': '',
    'resume': False, 'state_file': '', 'undo_log': '', 'insecure': False,
    'ai_base_url': '', 'ai_api_key': '', 'ai_model': '', 'ai_sleep': 0.8, 'no_ai': False,
    'ai_infer_season': False, 'default_season': 1, 'protect_sxxeyy': False,
    'organize_enabled': False, 'target_root': '', 'scan_exclude_target': True,
    'exclude_roots': [],
    'move_individual': True, 'on_conflict': 'suffix', 'alist_refresh': False,
    'delete_empty_source_dirs': False,
    'skip_exact_duplicate_files': True,
    'alist_sleep_read': 0.8, 'alist_sleep_write': 1.2, 'alist_retries': 5,
    'alist_retry_base': 0.8, 'alist_retry_max': 10.0,
    'category_buckets': ['电影', '剧集', '动漫', '纪录片', '综艺', '演唱会', '体育'],
    'region_buckets': ['大陆', '港台', '欧美', '日韩', '其他'],
    'category_region_map': {'电影': ['大陆', '港台', '欧美', '日韩', '其他'], '剧集': ['大陆', '港台', '欧美', '日韩', '其他'], '动漫': ['大陆', '港台', '欧美', '日韩', '其他'], '纪录片': [], '综艺': [], '演唱会': [], '体育': []},
    'init_target_tree': True,
    'log_host': '127.0.0.1', 'log_port': 55255, 'log_keep': 500,
    'log_web': False, 'cli_mode': False,
    'public_host': '', 'readonly_token': '', 'admin_password_hash': '', 'session_secret': '',
}
CURRENT_RUNTIME_CONFIG: Dict[str, Any] = dict(DEFAULTS)


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _to_list(value: Any) -> List[str]:
    if value in (None, ''):
        return []
    if isinstance(value, list):
        items = value
    else:
        items = str(value).replace('\r', '\n').replace('，', ',').split('\n')
    out: List[str] = []
    for item in items:
        parts = str(item).replace('，', ',').split(',')
        for part in parts:
            s = part.strip()
            if s:
                out.append(s)
    return out


def _to_category_region_map(value: Any) -> Dict[str, List[str]]:
    default_regions = list(DEFAULTS['region_buckets'])
    default_map = copy.deepcopy(DEFAULTS['category_region_map'])
    if value in (None, ''):
        return default_map
    if isinstance(value, list):
        out: Dict[str, List[str]] = {}
        for item in value:
            key = str(item or '').strip()
            if key:
                out[key] = list(default_regions)
        return out or default_map
    if isinstance(value, dict):
        out: Dict[str, List[str]] = {}
        for k, v in value.items():
            key = str(k or '').strip()
            if not key:
                continue
            if isinstance(v, bool):
                if v:
                    out[key] = list(default_regions)
                continue
            vals = v if isinstance(v, list) else str(v or '').split(',')
            cleaned = [str(x).strip() for x in vals if str(x).strip()]
            if cleaned:
                out[key] = cleaned
        return out or default_map
    text = str(value).strip()
    if not text:
        return default_map
    try:
        obj = json.loads(text)
    except Exception:
        obj = None
    if obj is not None:
        return _to_category_region_map(obj)
    out: Dict[str, List[str]] = {}
    for part in text.split(';'):
        part = part.strip()
        if not part:
            continue
        if ':' in part:
            cat, vals = part.split(':', 1)
            key = cat.strip()
            cleaned = [str(x).strip() for x in vals.split(',') if str(x).strip()]
            if key and cleaned:
                out[key] = cleaned
        else:
            key = part.strip()
            if key:
                out[key] = list(default_regions)
    return out or default_map


def _coerce_value(key: str, value: Any) -> Any:
    if value is None:
        return DEFAULTS.get(key)
    if key in BOOL_FIELDS:
        return _to_bool(value)
    if key in {'max_series', 'log_port', 'log_keep'}:
        try:
            return int(value)
        except Exception:
            return int(DEFAULTS.get(key, 0) or 0)
    if key in {'sleep', 'tmdb_sleep', 'ai_sleep'}:
        try:
            return float(value)
        except Exception:
            return float(DEFAULTS.get(key, 0) or 0)
    if key in {'roots', 'discover_categories', 'category_buckets', 'region_buckets', 'exclude_roots'}:
        return _to_list(value)
    if key == 'category_region_map':
        return _to_category_region_map(value)
    if isinstance(value, str):
        return value.strip()
    return value


def _mask(value: str) -> str:
    if not value:
        return ''
    if len(value) <= 6:
        return '*' * len(value)
    return value[:2] + '*' * (len(value) - 4) + value[-2:]


def _looks_masked(value: Any) -> bool:
    s = str(value or '').strip()
    if not s or '*' not in s:
        return False
    return bool(re.fullmatch(r'\*+|.{0,4}\*+.{0,4}', s))


def hash_password(password: str) -> str:
    password = (password or '').encode('utf-8')
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac('sha256', password, salt, 120000)
    return 'pbkdf2_sha256$120000$' + base64.b64encode(salt).decode() + '$' + base64.b64encode(digest).decode()


def verify_password(password: str, encoded: str) -> bool:
    try:
        algo, rounds, salt_b64, digest_b64 = (encoded or '').split('$', 3)
        if algo != 'pbkdf2_sha256':
            return False
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(digest_b64)
        actual = hashlib.pbkdf2_hmac('sha256', (password or '').encode('utf-8'), salt, int(rounds))
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def apply_runtime_config(config: Dict[str, Any] | None) -> Dict[str, Any]:
    """Apply runtime config in-place so existing imports keep seeing updates."""
    merged = dict(DEFAULTS)
    if config:
        for k, v in dict(config).items():
            if k in DEFAULTS:
                merged[k] = _coerce_value(k, v)
    if not merged.get('session_secret'):
        merged['session_secret'] = secrets.token_urlsafe(32)
    CURRENT_RUNTIME_CONFIG.clear()
    CURRENT_RUNTIME_CONFIG.update(merged)
    return copy.deepcopy(CURRENT_RUNTIME_CONFIG)


class RuntimeConfigStore:
    def __init__(self, config_dir: str):
        self.config_dir = os.path.abspath(config_dir)
        self.config_path = os.path.join(self.config_dir, 'config.json')

    def _normalize_loaded(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not data.get('session_secret'):
            data['session_secret'] = secrets.token_urlsafe(32)
        if 'rename_series' not in data:
            data['rename_series'] = True
        if 'rename_files' not in data:
            data['rename_files'] = True
        if 'fix_bare_sxxeyy' not in data:
            data['fix_bare_sxxeyy'] = True
        return data

    def load(self) -> Dict[str, Any]:
        data = dict(DEFAULTS)
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    raw = json.load(f) or {}
                for k, v in raw.items():
                    if k in DEFAULTS:
                        data[k] = _coerce_value(k, v)
            except Exception:
                pass
        return self._normalize_loaded(data)

    def load_with_env(self) -> Dict[str, Any]:
        return self.load()

    def merge_cli_overrides(self, base: Dict[str, Any], args: argparse.Namespace, *, include_empty: bool = False) -> Dict[str, Any]:
        ns = vars(args)
        cfg = dict(base or self.load())
        mapping = {
            'no_rename_series': ('rename_series', lambda v: not bool(v)),
            'no_rename_files': ('rename_files', lambda v: not bool(v)),
        }
        for k, v in ns.items():
            if k in mapping:
                if not v:
                    continue
                nk, fn = mapping[k]
                cfg[nk] = fn(v)
                continue
            if k not in DEFAULTS:
                continue
            if (not include_empty) and v in (None, ''):
                continue
            cfg[k] = _coerce_value(k, v)
        return self._normalize_loaded(cfg)

    def merge_payload(self, base: Dict[str, Any] | None, payload: Dict[str, Any] | None, *, include_empty: bool = False) -> Dict[str, Any]:
        cfg = dict(base or self.load())
        data = dict(payload or {})
        for k in DEFAULTS:
            if k not in data:
                continue
            v = data.get(k)
            if (not include_empty) and v in (None, ''):
                continue
            if k in SENSITIVE_FIELDS and _looks_masked(v):
                continue
            cfg[k] = _coerce_value(k, v)
        return self._normalize_loaded(cfg)

    def save(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        cur = self.load()
        new = dict(cur)
        for k in DEFAULTS:
            if k not in payload:
                continue
            v = payload.get(k)
            if k in SENSITIVE_FIELDS:
                if v is None or v == '':
                    continue
                if _looks_masked(v):
                    continue
            new[k] = _coerce_value(k, v)
        os.makedirs(self.config_dir, exist_ok=True)
        new = self._normalize_loaded(new)
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(new, f, ensure_ascii=False, indent=2)
        return new

    def masked_config(self) -> Dict[str, Any]:
        cfg = self.load()
        out = dict(cfg)
        for k in SENSITIVE_FIELDS:
            if k in out:
                out[k] = _mask(str(out.get(k) or ''))
        return out

    def get_admin_password(self) -> str:
        return str(self.load().get('admin_password_hash') or '')

    def get_public_host(self) -> str:
        return str(self.load().get('public_host') or '')

    def args_to_config(self, args: argparse.Namespace) -> Dict[str, Any]:
        return self.merge_cli_overrides(self.load(), args)

    def config_to_argv(self, cfg: Dict[str, Any]) -> List[str]:
        c = dict(self.load())
        c.update(cfg or {})
        argv: List[str] = []
        arg_map = {
            'alist_url': '--alist-url', 'alist_token': '--alist-token', 'alist_user': '--alist-user', 'alist_pass': '--alist-pass', 'alist_otp': '--alist-otp',
            'tmdb_key': '--tmdb-key', 'tmdb_lang': '--tmdb-lang', 'roots': '--roots', 'discover_root_regex': '--discover-root-regex',
            'discover_categories': '--discover-categories', 'keyword': '--keyword', 'max_series': '--max-series', 'season_format': '--season-format',
            'sleep': '--sleep', 'tmdb_sleep': '--tmdb-sleep', 'skip_dir_regex': '--skip-dir-regex', 'state_file': '--state-file', 'undo_log': '--undo-log',
            'ai_base_url': '--ai-base-url', 'ai_api_key': '--ai-api-key', 'ai_model': '--ai-model', 'ai_sleep': '--ai-sleep',
            'target_root': '--target-root', 'exclude_roots': '--exclude-roots', 'category_buckets': '--category-buckets', 'region_buckets': '--region-buckets',
            'log_host': '--log-host', 'log_port': '--log-port', 'log_keep': '--log-keep',
        }
        for k, flag in arg_map.items():
            v = c.get(k)
            if v in (None, '', []):
                continue
            if isinstance(v, list):
                argv.extend([flag, ','.join(str(x) for x in v if str(x).strip())])
            else:
                argv.extend([flag, str(v)])
        crm = c.get('category_region_map')
        if isinstance(crm, dict) and crm:
            parts = []
            for cat, regions in crm.items():
                cat_s = str(cat).strip()
                if not cat_s:
                    continue
                if isinstance(regions, list):
                    reg_s = ','.join(str(x).strip() for x in regions if str(x).strip())
                else:
                    reg_s = str(regions).strip()
                parts.append(f"{cat_s}:{reg_s}" if reg_s else cat_s)
            if parts:
                argv.extend(['--category-region-map', ';'.join(parts)])
        if c.get('auto_roots'):
            argv.append('--auto-roots')
        if c.get('dry_run'):
            argv.append('--dry-run')
        if c.get('resume'):
            argv.append('--resume')
        else:
            argv.append('--no-resume')
        if c.get('insecure'):
            argv.append('--insecure')
        if c.get('no_ai'):
            argv.append('--no-ai')
        if c.get('organize_enabled'):
            argv.append('--organize-enabled')
        if c.get('scan_exclude_target'):
            argv.append('--scan-exclude-target')
        if c.get('init_target_tree'):
            argv.append('--init-target-tree')
        if not c.get('rename_series', True):
            argv.append('--no-rename-series')
        if not c.get('rename_files', True):
            argv.append('--no-rename-files')
        if c.get('fix_bare_sxxeyy', True):
            argv.append('--fix-bare-sxxeyy')
        if not c.get('skip_exact_duplicate_files', True):
            argv.append('--no-skip-exact-duplicate-files')
        return argv
