from collections.abc import Callable

import httpx
import pytest

from climate_lakehouse.ingest.http import HttpClient, RateLimiter


def _client(
    handler: Callable[[httpx.Request], httpx.Response], sleeps: list[float], retries: int = 3
) -> HttpClient:
    return HttpClient(
        timeout_s=1,
        max_retries=retries,
        requests_per_s=1000,
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    )


def _sequence(*responses: httpx.Response | Exception) -> Callable[[httpx.Request], httpx.Response]:
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    return handler


def test_returns_body_on_success() -> None:
    sleeps: list[float] = []
    client = _client(_sequence(httpx.Response(200, content=b"data")), sleeps)
    assert client.get("https://x.test/a") == b"data"
    assert sleeps == []


def test_retries_server_errors_and_timeouts_with_growing_backoff() -> None:
    sleeps: list[float] = []
    client = _client(
        _sequence(
            httpx.Response(503),
            httpx.ReadTimeout("slow"),
            httpx.ConnectError("refused"),
            httpx.Response(200, content=b"ok"),
        ),
        sleeps,
    )
    assert client.get("https://x.test/a") == b"ok"
    assert len(sleeps) == 3
    # full jitter: delay for attempt n lies in [0, base * 2**n]
    assert all(0 <= s <= 2**i for i, s in enumerate(sleeps))


def test_does_not_retry_client_errors() -> None:
    sleeps: list[float] = []
    client = _client(_sequence(httpx.Response(404)), sleeps)
    with pytest.raises(httpx.HTTPStatusError, match="404"):
        client.get("https://x.test/missing")
    assert sleeps == []


def test_gives_up_after_max_retries() -> None:
    sleeps: list[float] = []
    client = _client(_sequence(*[httpx.Response(500)] * 3), sleeps, retries=2)
    with pytest.raises(httpx.HTTPStatusError, match="HTTP 500"):
        client.get("https://x.test/a")
    assert len(sleeps) == 2


def test_honours_retry_after_header() -> None:
    sleeps: list[float] = []
    client = _client(
        _sequence(httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200)), sleeps
    )
    client.get("https://x.test/a")
    assert sleeps == [7.0]


def test_rate_limiter_spaces_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr("climate_lakehouse.ingest.http.time.sleep", slept.append)
    limiter = RateLimiter(per_second=4, clock=lambda: 100.0)
    for _ in range(3):
        limiter.wait()
    assert slept == [0.25, 0.5]
