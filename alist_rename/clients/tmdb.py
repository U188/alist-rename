"""TMDB API client."""
from time import sleep
from typing import Any, Dict, List

import requests

from alist_rename.config import CURRENT_RUNTIME_CONFIG
from alist_rename.common.rate_limit import RateLimiter

class TMDBClient:
    def __init__(self, api_key: str, language: str = "zh-CN", sleep: float = 0.3, timeout: float = 20.0):
        self.api_key = api_key
        self.language = language
        self.timeout = timeout
        self.rl_read = RateLimiter(float(CURRENT_RUNTIME_CONFIG.get("alist_sleep_read", sleep)))
        self.rl_write = RateLimiter(float(CURRENT_RUNTIME_CONFIG.get("alist_sleep_write", max(1.2, float(sleep)))))
        self.retries = int(CURRENT_RUNTIME_CONFIG.get("alist_retries", 5))
        self.retry_base = float(CURRENT_RUNTIME_CONFIG.get("alist_retry_base", 0.8))
        self.retry_max = float(CURRENT_RUNTIME_CONFIG.get("alist_retry_max", 10.0))
        # TMDB 在部分网络环境可能无法直连。
        # 你现在用的代理形态通常分两类：
        #   1) 官方：  https://api.themoviedb.org/3/...
        #   2) 代理：  https://<proxy>/get/...   （把 /get/ 映射到官方 /3/）
        #
        # 约定（按你的说明）：
        #   - https://www.example.com/get/  <=>  https://api.themoviedb.org/3/
        #   - https://www.example.com/img/  <=>  https://image.tmdb.org/
        #
        # 因此：
        #   - 你填 api.themoviedb.org（或 themoviedb.org）时，自动补 /3
        #   - 你填其它域名（如 tmdb.melonhu.cn）时，自动补 /get
        #   - 若你已经显式写了 /get 或 /3，就保持不变
        base = str(CURRENT_RUNTIME_CONFIG.get("tmdb_api_base", "") or "").strip()
        if base:
            base = base.rstrip("/")
            if base.endswith("/get") or base.endswith("/3"):
                self.base = base
            else:
                low = base.lower()
                # 官方域名：自动补 /3
                if "themoviedb.org" in low:
                    self.base = base + "/3"
                else:
                    # 代理域名：自动补 /get
                    self.base = base + "/get"
        else:
            self.base = "https://api.themoviedb.org/3"

    def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self.rl_read.wait()
        url = self.base + path
        params = dict(params)
        params["api_key"] = self.api_key
        r = requests.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def search_tv(self, query: str) -> List[Dict[str, Any]]:
        return (self.get("/search/tv", {"query": query, "language": self.language}).get("results") or [])

    def tv_details(self, tv_id: int) -> Dict[str, Any]:
        return self.get(f"/tv/{tv_id}", {"language": self.language})

__all__ = ["TMDBClient"]
