"""Parsing of DWD open-data artefacts: directory listings, station list, file names."""

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

STATION_LIST_NAME = "KL_Tageswerte_Beschreibung_Stationen.txt"
STATION_LIST_COLUMNS = (
    "Stations_id",
    "von_datum",
    "bis_datum",
    "Stationshoehe",
    "geoBreite",
    "geoLaenge",
    "Stationsname",
    "Bundesland",
    "Abgabe",
)

# nginx autoindex row: <a href="name">name</a>   28-Oct-2026 09:05:14   34407
_LISTING_ROW = re.compile(
    r'<a href="([^"/]+)">[^<]*</a>\s+(\d{2}-\w{3}-\d{4} \d{2}:\d{2}(?::\d{2})?)\s+(\d+)'
)
_STATION_IN_NAME = re.compile(r"^tageswerte_KL_(\d{5})_")
# id, from, to, then height/lat/lon (skipped), then the free-text columns
_STATION_ROW = re.compile(r"^(\d+) +(\d{8}) +(\d{8}) +-?\d+ +-?[\d.]+ +-?[\d.]+ (.*)$")


class FormatError(ValueError):
    """Source data does not have the structure the pipeline expects."""


@dataclass(frozen=True)
class RemoteFile:
    name: str
    url: str
    last_modified: str  # as printed by the server; only compared for equality
    size: int

    @property
    def station_id(self) -> str | None:
        match = _STATION_IN_NAME.match(self.name)
        return match.group(1) if match else None


@dataclass(frozen=True)
class Station:
    station_id: str
    from_date: date
    to_date: date
    name: str
    state: str


def parse_listing(html: str, base_url: str) -> list[RemoteFile]:
    base = base_url.rstrip("/")
    return [
        RemoteFile(name=name, url=f"{base}/{name}", last_modified=modified, size=int(size))
        for name, modified, size in _LISTING_ROW.findall(html)
    ]


def parse_station_list(text: str) -> list[Station]:
    """Parse the station list.

    The dashed separator does not match the data columns, so rows are split by content: six
    numeric fields separated by spaces, then name, state and an optional "Abgabe" column
    separated by at least two spaces (names are at most 36 characters in a 41-character field).
    """
    lines = text.splitlines()
    if not lines or tuple(lines[0].split()) != STATION_LIST_COLUMNS:
        raise FormatError(
            f"station list header changed: expected {STATION_LIST_COLUMNS}, "
            f"got {tuple(lines[0].split()) if lines else ()}"
        )

    stations = []
    for number, line in enumerate(lines[2:], start=3):
        if not line.strip():
            continue
        match = _STATION_ROW.match(line)
        texts = re.split(r" {2,}", match.group(4).strip()) if match else []
        if match is None or len(texts) < 2:
            raise FormatError(f"station list line {number} has an unexpected layout: {line!r}")
        stations.append(
            Station(
                station_id=match.group(1).zfill(5),
                from_date=_parse_yyyymmdd(match.group(2)),
                to_date=_parse_yyyymmdd(match.group(3)),
                name=texts[0],
                state=texts[1],
            )
        )
    return stations


def select_stations(
    stations: list[Station], limit: int, reference_start: date, active_since: date
) -> list[str]:
    """Pick up to `limit` stations that cover the reference period and still report.

    Stations are taken round-robin per federal state so that a small sample still covers every
    region. `limit=0` returns all stations without filtering.
    """
    if limit == 0:
        return sorted(s.station_id for s in stations)

    by_state: dict[str, list[str]] = defaultdict(list)
    for s in sorted(stations, key=lambda s: s.station_id):
        if s.from_date <= reference_start and s.to_date >= active_since:
            by_state[s.state].append(s.station_id)

    selected: list[str] = []
    queues = [by_state[state] for state in sorted(by_state)]
    while len(selected) < limit and any(queues):
        for queue in queues:
            if queue and len(selected) < limit:
                selected.append(queue.pop(0))
    return sorted(selected)


def _parse_yyyymmdd(value: str) -> date:
    try:
        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
    except ValueError as exc:
        raise FormatError(f"invalid date {value!r}: {exc}") from exc
