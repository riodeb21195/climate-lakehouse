from datetime import date

import pytest

from climate_lakehouse.ingest.dwd import (
    FormatError,
    Station,
    parse_listing,
    parse_station_list,
    select_stations,
)
from tests.conftest import read_fixture

BASE = "https://example.test/recent"


def test_parse_listing_reads_name_timestamp_and_size() -> None:
    files = parse_listing(read_fixture("listing_recent.html").decode(), BASE + "/")

    assert [f.name for f in files] == [
        "KL_Tageswerte_Beschreibung_Stationen.txt",
        "KL_Tageswerte_Beschreibung_Stationen_partner.txt",
        "tageswerte_KL_00044_akt.zip",
        "tageswerte_KL_15194_akt.zip",
    ]
    akt = files[-1]
    assert akt.url == f"{BASE}/tageswerte_KL_15194_akt.zip"
    assert akt.last_modified == "28-Oct-2026 09:05:14"
    assert akt.size == 9764
    assert akt.station_id == "15194"
    assert files[0].station_id is None


def test_parse_listing_skips_parent_link() -> None:
    files = parse_listing(read_fixture("listing_historical.html").decode(), BASE)
    assert all(not f.name.startswith("..") for f in files)
    assert len(files) == 2


def test_parse_station_list_handles_fixed_width_and_latin1() -> None:
    stations = parse_station_list(
        read_fixture("KL_Tageswerte_Beschreibung_Stationen.txt").decode("latin-1")
    )

    assert len(stations) == 10
    by_id = {s.station_id: s for s in stations}
    assert by_id["04928"].name == "Stuttgart (Schnarrenberg)"
    assert by_id["04928"].state == "Baden-Württemberg"
    assert by_id["15194"].name == "Börßum-Achim"
    assert by_id["00003"].to_date == date(2011, 3, 31)
    assert (by_id["01048"].height_m, by_id["01048"].latitude, by_id["01048"].longitude) == (
        228,
        51.1278,
        13.7543,
    )


def test_parse_station_list_rejects_changed_header() -> None:
    text = read_fixture("KL_Tageswerte_Beschreibung_Stationen.txt").decode("latin-1")
    with pytest.raises(FormatError, match="header changed"):
        parse_station_list(text.replace("Bundesland", "Land", 1))


def test_parse_station_list_reports_bad_line() -> None:
    text = read_fixture("KL_Tageswerte_Beschreibung_Stationen.txt").decode("latin-1")
    with pytest.raises(FormatError, match="line 3"):
        parse_station_list(text.replace("18910101", "1891-1-1", 1))


def _station(sid: str, state: str, start: int = 1950, end: int = 2026) -> Station:
    return Station(sid, date(start, 1, 1), date(end, 10, 1), 0, 50.0, 10.0, f"S{sid}", state)


def test_select_stations_round_robin_over_states() -> None:
    stations = [
        _station("00001", "Bayern"),
        _station("00002", "Bayern"),
        _station("00003", "Bayern"),
        _station("00004", "Berlin"),
        _station("00005", "Hamburg"),
    ]
    assert select_stations(stations, 3, date(1961, 1, 1), date(2026, 9, 1)) == [
        "00001",
        "00004",
        "00005",
    ]


def test_select_stations_requires_reference_period_and_activity() -> None:
    stations = [
        _station("00001", "Bayern", start=1970),  # misses reference period
        _station("00002", "Bayern", end=2011),  # closed
        _station("00003", "Bayern"),
    ]
    assert select_stations(stations, 10, date(1961, 1, 1), date(2026, 9, 1)) == ["00003"]


def test_select_stations_zero_means_all() -> None:
    stations = [_station("00002", "Bayern", end=2000), _station("00001", "Berlin", start=2020)]
    assert select_stations(stations, 0, date(1961, 1, 1), date(2026, 9, 1)) == ["00001", "00002"]
