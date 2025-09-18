from __future__ import annotations

import threading
import time


class TokenBucket:
    def __init__(self, rate_per_second: float, capacity: int | None = None) -> None:
        self.rate = float(rate_per_second)
        self.capacity = float(capacity if capacity is not None else rate_per_second)
        self.tokens = self.capacity
        self.timestamp = time.monotonic()
        self.lock = threading.Lock()

    def consume(self, tokens: float = 1.0) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.timestamp
                self.timestamp = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                # Need to wait
                needed = tokens - self.tokens
                sleep_for = needed / self.rate
            time.sleep(sleep_for)

