"""Inkrementeller, fortsetzbarer Download der DWD-Tageswerte in die Raw-Zone.

Eine Datei wird nur geladen, wenn die Raw-Zone nicht schon denselben Quellstand hat. Ein
abgebrochener Lauf macht deshalb dort weiter, wo er stand, ein doppelter Lauf überträgt nichts.
"""

import logging
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta

import httpx

from climate_lakehouse.alerts import send_alert
from climate_lakehouse.config import Settings, get_settings
from climate_lakehouse.ingest.dwd import (
    STATION_LIST_NAME,
    FormatError,
    RemoteFile,
    parse_listing,
    parse_station_list,
    select_stations,
)
from climate_lakehouse.ingest.http import HttpClient
from climate_lakehouse.ingest.storage import RawStore, S3RawStore, raw_key
from climate_lakehouse.ingest.validate import validate_kl_zip
from climate_lakehouse.logging import configure_logging

log = logging.getLogger(__name__)

REFERENCE_PERIOD_START = date(1961, 1, 1)
ACTIVE_WITHIN = timedelta(days=60)


@dataclass
class DownloadSummary:
    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    stations_selected: int = 0
    files_planned: int = 0
    files_downloaded: int = 0
    files_skipped: int = 0
    bytes_downloaded: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def files_failed(self) -> int:
        return len(self.errors)

    @property
    def status(self) -> str:
        return "failed" if self.errors else "succeeded"


def run_download(
    settings: Settings, http: HttpClient, store: RawStore, today: date | None = None
) -> DownloadSummary:
    today = today or datetime.now(UTC).date()
    summary = DownloadSummary(run_id=str(uuid.uuid4()), started_at=datetime.now(UTC))
    base = settings.dwd_base_url.rstrip("/")

    historical = parse_listing(http.get(f"{base}/historical/").decode(), f"{base}/historical")
    recent = parse_listing(http.get(f"{base}/recent/").decode(), f"{base}/recent")
    station_file = next((f for f in recent if f.name == STATION_LIST_NAME), None)
    if station_file is None:
        raise FormatError(f"{STATION_LIST_NAME} missing from {base}/recent/")

    station_text = http.get(station_file.url)
    stations = parse_station_list(station_text.decode("latin-1"))
    selected = set(
        select_stations(
            stations, settings.station_limit, REFERENCE_PERIOD_START, today - ACTIVE_WITHIN
        )
    )
    summary.stations_selected = len(selected)

    targets = [("historical", f) for f in historical if f.station_id in selected]
    targets += [("recent", f) for f in recent if f.station_id in selected]
    summary.files_planned = len(targets) + 1

    station_key = raw_key("stations", station_file.name)
    if store.is_current(station_key, station_file):
        summary.files_skipped += 1
    else:
        store.put(station_key, station_text, station_file, summary.run_id)
        summary.files_downloaded += 1
        summary.bytes_downloaded += len(station_text)

    def fetch(target: tuple[str, RemoteFile]) -> int | str | None:
        kind, remote = target
        return _process(kind, remote, http, store, summary.run_id)

    with ThreadPoolExecutor(max_workers=settings.download_workers) as pool:
        for outcome in pool.map(fetch, targets):
            if isinstance(outcome, str):
                summary.errors.append(outcome)
            elif outcome is None:
                summary.files_skipped += 1
            else:
                summary.files_downloaded += 1
                summary.bytes_downloaded += outcome

    summary.finished_at = datetime.now(UTC)
    return summary


def _process(
    kind: str, remote: RemoteFile, http: HttpClient, store: RawStore, run_id: str
) -> int | str | None:
    """Lädt ein Archiv: gespeicherte Bytes, None wenn schon aktuell, sonst eine Fehlermeldung."""
    key = raw_key(kind, remote.name)
    try:
        if store.is_current(key, remote):
            return None
        body = http.get(remote.url)
        validate_kl_zip(remote.name, body, remote.station_id or "")
        store.put(key, body, remote, run_id)
        log.info("stored", extra={"key": key, "bytes": len(body)})
        return len(body)
    except (FormatError, httpx.HTTPError) as exc:
        log.error("file failed", extra={"file": remote.name, "error": str(exc)})
        return f"{remote.name}: {exc}"


def main() -> int:
    configure_logging()
    settings = get_settings()
    http = HttpClient(
        timeout_s=settings.http_timeout_s,
        max_retries=settings.http_max_retries,
        requests_per_s=settings.http_requests_per_s,
    )
    store = S3RawStore.from_settings(settings)
    try:
        summary = run_download(settings, http, store)
    finally:
        http.close()
    log.info(
        "download finished",
        extra={**asdict(summary), "status": summary.status, "files_failed": summary.files_failed},
    )
    if summary.errors:
        send_alert(
            settings,
            f"download failed for {summary.files_failed} files (run {summary.run_id})",
            "\n".join(summary.errors[:20]),
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
