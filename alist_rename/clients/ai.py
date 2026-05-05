"""OpenAI-compatible AI client."""
import json
import logging
import time
from time import sleep
from typing import Any, Dict, Optional

import requests

from alist_rename.config import CURRENT_RUNTIME_CONFIG
from alist_rename.common.paths import now_ts
from alist_rename.common.rate_limit import RateLimiter

logger = logging.getLogger("embyrename")

class AIClient:
    """OpenAI-compatible /v1/chat/completions client (optional)."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        sleep: float = 1.0,
        timeout: float = 60.0,
        verify_tls: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.verify_tls = verify_tls
        self.rl_read = RateLimiter(float(CURRENT_RUNTIME_CONFIG.get("alist_sleep_read", sleep)))
        self.rl_write = RateLimiter(float(CURRENT_RUNTIME_CONFIG.get("alist_sleep_write", max(1.2, float(sleep)))))
        self.retries = int(CURRENT_RUNTIME_CONFIG.get("alist_retries", 5))
        self.retry_base = float(CURRENT_RUNTIME_CONFIG.get("alist_retry_base", 0.8))
        self.retry_max = float(CURRENT_RUNTIME_CONFIG.get("alist_retry_max", 10.0))
        self.last_error: Optional[Dict[str, Any]] = None

    def _set_last_error(self, kind: str, message: str, status_code: Optional[int] = None, retryable: bool = False):
        self.last_error = {
            "kind": kind,
            "message": message,
            "status_code": status_code,
            "retryable": bool(retryable),
            "at": now_ts(),
        }

    def consume_last_error(self) -> Optional[Dict[str, Any]]:
        err = self.last_error
        self.last_error = None
        return err

    def _parse_json_from_text(self, text: str) -> Optional[dict]:
        text = text.strip()
        # best-effort: extract first {...}
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            chunk = text[start : end + 1]
            try:
                return json.loads(chunk)
            except Exception:
                return None
        return None

    def chat_json(self, system: str, user: str, json_mode: bool = True, max_tokens: int = 400) -> Optional[dict]:
        """Return a JSON object (or None)."""
        self.last_error = None
        self.rl_read.wait()
        url = self.base_url + ("/chat/completions" if self.base_url.rstrip("/").endswith("/v1") else "/v1/chat/completions")
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0,
            "max_tokens": max_tokens,
        }
        if json_mode:
            payload["response_format"] = {"type": "json_object"}

        attempts = max(1, min(int(self.retries or 1), 3))
        for attempt in range(1, attempts + 1):
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=self.timeout, verify=self.verify_tls)
            except requests.exceptions.Timeout as e:
                retryable = attempt < attempts
                msg = f"AI timeout: {e}"
                self._set_last_error("timeout", msg, retryable=retryable)
                logger.warning("[AI] %s (attempt %s/%s)", msg, attempt, attempts)
                if retryable:
                    time.sleep(min(self.retry_max, self.retry_base * (2 ** (attempt - 1))))
                    continue
                return None
            except requests.exceptions.ConnectionError as e:
                retryable = attempt < attempts
                msg = f"AI connection error: {e}"
                self._set_last_error("connection", msg, retryable=retryable)
                logger.warning("[AI] %s (attempt %s/%s)", msg, attempt, attempts)
                if retryable:
                    time.sleep(min(self.retry_max, self.retry_base * (2 ** (attempt - 1))))
                    continue
                return None
            except requests.exceptions.RequestException as e:
                self._set_last_error("request", f"AI request error: {e}", retryable=False)
                logger.warning("[AI] request error: %s", e)
                return None

            status = int(r.status_code)
            body_preview = (r.text or "").strip().replace("\n", " ")[:240]
            if status >= 400:
                if status in (401, 403):
                    self._set_last_error("auth", f"AI auth failed HTTP {status}: check api_key/permission", status_code=status, retryable=False)
                    logger.warning("[AI] auth failed HTTP %s | body=%s", status, body_preview)
                    return None
                if status == 404:
                    self._set_last_error("endpoint", f"AI endpoint/model not found HTTP 404", status_code=status, retryable=False)
                    logger.warning("[AI] endpoint/model not found HTTP 404 | url=%s | body=%s", url, body_preview)
                    return None
                if status == 429:
                    retryable = attempt < attempts
                    self._set_last_error("rate_limit", f"AI rate limited HTTP 429", status_code=status, retryable=retryable)
                    logger.warning("[AI] rate limited HTTP 429 (attempt %s/%s) | body=%s", attempt, attempts, body_preview)
                    if retryable:
                        wait_s = min(self.retry_max, self.retry_base * (2 ** (attempt - 1)))
                        ra = r.headers.get("Retry-After")
                        if ra:
                            try:
                                wait_s = min(self.retry_max, max(wait_s, float(ra)))
                            except Exception:
                                pass
                        time.sleep(wait_s)
                        continue
                    return None
                if 500 <= status <= 599:
                    retryable = attempt < attempts
                    self._set_last_error("server", f"AI upstream server error HTTP {status}", status_code=status, retryable=retryable)
                    logger.warning("[AI] upstream server error HTTP %s (attempt %s/%s) | body=%s", status, attempt, attempts, body_preview)
                    if retryable:
                        time.sleep(min(self.retry_max, self.retry_base * (2 ** (attempt - 1))))
                        continue
                    return None
                self._set_last_error("http", f"AI HTTP {status}", status_code=status, retryable=False)
                logger.warning("[AI] HTTP %s | body=%s", status, body_preview)
                return None

            try:
                data = r.json()
            except Exception as e:
                self._set_last_error("bad_json", f"AI response is not valid JSON: {e}", status_code=status, retryable=False)
                logger.warning("[AI] invalid response JSON: %s | body=%s", e, body_preview)
                return None
            try:
                content = data["choices"][0]["message"]["content"]
            except Exception:
                self._set_last_error("bad_payload", "AI response missing choices[0].message.content", status_code=status, retryable=False)
                logger.warning("[AI] response missing content | data=%s", str(data)[:240])
                return None
            parsed = self._parse_json_from_text(content)
            if parsed is None:
                self._set_last_error("bad_content", "AI content did not contain valid JSON object", status_code=status, retryable=False)
                logger.warning("[AI] content is not parseable JSON | content=%s", str(content)[:240])
                return None
            self.last_error = None
            return parsed
        return None

__all__ = ["AIClient"]
