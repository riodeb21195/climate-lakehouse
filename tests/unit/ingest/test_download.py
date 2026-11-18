from datetime import date

import pytest

from climate_lakehouse.config import Settings
from climate_lakehouse.ingest import download
from climate_lakehouse.ingest.dwd import FormatError
from climate_lakehouse.ingest.http import HttpClient
from climate_lakehouse.ingest.storage import S3RawStore
from tests.conftest import HIST_ZIP, RECENT_ZIP
from tests.unit.ingest.fakes import BASE, MemoryStore, dwd_transport

TODAY = date(2026, 10, 28)


def _settings(limit: int) -> Settings:
    return Settings(_env_file=None, station_limit=limit, dwd_base_url=BASE)  # type: ignore[call-arg]


def _http(overrides: dict[str, bytes] | None = None) -> HttpClient:
    return HttpClient(1, 0, 1000, transport=dwd_transport(overrides), sleep=lambda _: None)


def test_downloads_selected_station_and_reports_missing_file() -> None:
    store = MemoryStore()
    # limit 0 wählt alle 10 Fixture-Stationen; nur 15194 und 00044 haben gelistete Dateien
    summary = download.run_download(_settings(0), _http(), store, today=TODAY)

    assert summary.stations_selected == 10
    assert summary.files_planned == 5  # Stationsliste + 2 historical + 2 recent
    assert summary.files_downloaded == 3
    assert summary.status == "failed"
    assert len(summary.errors) == 2
    assert all("00044" in e and "404" in e for e in summary.errors)
    assert set(store.objects) == {
        "raw/dwd/daily_kl/stations/KL_Tageswerte_Beschreibung_Stationen.txt",
        f"raw/dwd/daily_kl/historical/{HIST_ZIP}",
        f"raw/dwd/daily_kl/recent/{RECENT_ZIP}",
    }
    assert summary.finished_at is not None


def test_second_run_transfers_nothing() -> None:
    store = MemoryStore()
    download.run_download(_settings(0), _http(), store, today=TODAY)
    puts_after_first = store.puts

    second = download.run_download(_settings(0), _http(), store, today=TODAY)

    assert store.puts == puts_after_first
    assert second.files_downloaded == 0
    assert second.files_skipped == 3


def test_invalid_archive_is_not_stored() -> None:
    store = MemoryStore()
    summary = download.run_download(
        _settings(0),
        _http({f"{BASE}/recent/{RECENT_ZIP}": b"<html>maintenance</html>"}),
        store,
        today=TODAY,
    )
    assert f"raw/dwd/daily_kl/recent/{RECENT_ZIP}" not in store.objects
    assert any("not a readable ZIP" in e for e in summary.errors)


def test_sample_selection_filters_by_reference_period() -> None:
    # 15194 beginnt 2024, eine Stichprobe (limit > 0) darf sie also nicht wählen
    store = MemoryStore()
    summary = download.run_download(_settings(3), _http(), store, today=TODAY)
    assert summary.stations_selected == 3
    assert not any("15194" in key for key in store.objects)


def test_missing_station_list_is_a_format_error() -> None:
    http = _http({f"{BASE}/recent/": b"<html><pre></pre></html>"})
    with pytest.raises(FormatError, match="KL_Tageswerte_Beschreibung_Stationen"):
        download.run_download(_settings(0), http, MemoryStore(), today=TODAY)


def test_main_returns_nonzero_when_files_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(download, "get_settings", lambda: _settings(0))
    monkeypatch.setattr(download, "HttpClient", lambda **_: _http())
    monkeypatch.setattr(S3RawStore, "from_settings", lambda *_: MemoryStore())
    assert download.main() == 1
