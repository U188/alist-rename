"""Rate limiting helpers."""
import time

class RateLimiter:
    def __init__(self, min_interval_sec: float):
        self.min_interval_sec = max(0.0, float(min_interval_sec))
        self._last = 0.0

    def wait(self):
        if self.min_interval_sec <= 0:
            return
        now = time.time()
        delta = now - self._last
        if delta < self.min_interval_sec:
            time.sleep(self.min_interval_sec - delta)
        self._last = time.time()

__all__ = ["RateLimiter"]
