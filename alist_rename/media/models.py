"""Media data models."""
import dataclasses
from typing import List, Optional

@dataclasses.dataclass
class DirEntry:
    name: str
    is_dir: bool

@dataclasses.dataclass
class SeriesMeta:
    tv_id: int
    name: str
    year: Optional[int]
    # season hint derived from original folder name, e.g. “第四季”
    season_hint: Optional[int] = None
    category: Optional[str] = None
    region: Optional[str] = None
    media_type: Optional[str] = None
    source_language: Optional[str] = None
    keywords: Optional[List[str]] = None
    ai_inferred: bool = False
    tmdb_confident: bool = True

__all__ = ["DirEntry", "SeriesMeta"]
