"""HTTP-GET mit Timeout, Rate Limit und exponentiellem Backoff nur bei wiederholbaren Fehlern."""

import logging
import random
import threading
import time
from collections.abc import Callable

import httpx

log = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
MAX_BACKOFF_S = 60.0


class RateLimiter:
    """Erlaubt höchstens `per_second` Aufrufe pro Sekunde über alle Threads."""

    def __init__(self, per_second: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._interval = 1.0 / per_second
        self._clock = clock
        self._next_slot = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self._interval
        if slot > now:
            time.sleep(slot - now)


class HttpClient:
    def __init__(
        self,
        timeout_s: float,
        max_retries: int,
        requests_per_s: float,
        backoff_base_s: float = 1.0,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._client = httpx.Client(timeout=timeout_s, transport=transport, follow_redirects=True)
        self._max_retries = max_retries
        self._backoff_base_s = backoff_base_s
        self._limiter = RateLimiter(requests_per_s)
        self._sleep = sleep

    def get(self, url: str) -> bytes:
        """Liefert den Body; wirft bei nicht wiederholbaren Fehlern oder nach dem letzten Retry."""
        attempt = 0
        while True:
            self._limiter.wait()
            try:
                response = self._client.get(url)
                if response.status_code not in RETRYABLE_STATUS:
                    response.raise_for_status()
                    return response.content
                error: Exception = httpx.HTTPStatusError(
                    f"HTTP {response.status_code}", request=response.request, response=response
                )
                retry_after = _retry_after_s(response)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                error, retry_after = exc, None

            if attempt >= self._max_retries:
                raise error
            delay = retry_after if retry_after is not None else self._backoff(attempt)
            log.warning(
                "retrying request",
                extra={"url": url, "attempt": attempt + 1, "delay_s": delay, "error": str(error)},
            )
            self._sleep(delay)
            attempt += 1

    def _backoff(self, attempt: int) -> float:
        # Full Jitter verhindert, dass parallele Worker im Gleichschritt wiederholen.
        return random.uniform(0, min(MAX_BACKOFF_S, self._backoff_base_s * 2**attempt))

    def close(self) -> None:
        self._client.close()


def _retry_after_s(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After", "")
    return min(float(value), MAX_BACKOFF_S) if value.isdigit() else None
