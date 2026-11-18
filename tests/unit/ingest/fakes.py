import httpx

from climate_lakehouse.ingest.dwd import RemoteFile
from tests.conftest import HIST_ZIP, RECENT_ZIP, read_fixture

BASE = "https://dwd.test/kl"


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, RemoteFile]] = {}
        self.puts = 0

    def is_current(self, key: str, remote: RemoteFile) -> bool:
        stored = self.objects.get(key)
        return stored is not None and (stored[1].last_modified, stored[1].size) == (
            remote.last_modified,
            remote.size,
        )

    def put(self, key: str, body: bytes, remote: RemoteFile, run_id: str) -> None:
        self.objects[key] = (body, remote)
        self.puts += 1


def dwd_transport(overrides: dict[str, bytes] | None = None) -> httpx.MockTransport:
    """Liefert die aufgezeichneten DWD-Fixtures; 00044 ist gelistet, das Archiv fehlt (404)."""
    routes = {
        f"{BASE}/historical/": read_fixture("listing_historical.html"),
        f"{BASE}/recent/": read_fixture("listing_recent.html"),
        f"{BASE}/recent/KL_Tageswerte_Beschreibung_Stationen.txt": read_fixture(
            "KL_Tageswerte_Beschreibung_Stationen.txt"
        ),
        f"{BASE}/historical/{HIST_ZIP}": read_fixture(HIST_ZIP),
        f"{BASE}/recent/{RECENT_ZIP}": read_fixture(RECENT_ZIP),
        **(overrides or {}),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body = routes.get(str(request.url))
        return httpx.Response(200, content=body) if body is not None else httpx.Response(404)

    return httpx.MockTransport(handler)
