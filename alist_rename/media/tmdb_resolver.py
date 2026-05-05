"""Backward-compatible resolver exports after package split."""
from alist_rename.scanner.discover import find_library_root, find_top_anchor_root
from alist_rename.media.resolver import (
    parse_boolish, parse_category_region_map, ensure_organize_tree,
    _norm_region_token, infer_category_region_from_tmdb, pick_organized_destination,
    ai_extract_query, ai_extract_queries, ai_choose_tmdb, ai_extract_media_meta,
    is_bad_tmdb_query, resolve_series,
)

__all__ = [
    "find_library_root", "find_top_anchor_root", "parse_boolish",
    "parse_category_region_map", "ensure_organize_tree", "_norm_region_token",
    "infer_category_region_from_tmdb", "pick_organized_destination",
    "ai_extract_query", "ai_extract_queries", "ai_choose_tmdb",
    "ai_extract_media_meta", "is_bad_tmdb_query", "resolve_series",
]
