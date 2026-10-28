from datetime import UTC, datetime

import pytest

from climate_lakehouse.ingest.storage import parse_listing_time, raw_object


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("17-Jun-2026 10:15:08", datetime(2026, 6, 17, 10, 15, 8, tzinfo=UTC)),
        ("17-Jun-2026 10:15", datetime(2026, 6, 17, 10, 15, tzinfo=UTC)),
    ],
)
def test_parse_listing_time(value: str, expected: datetime) -> None:
    assert parse_listing_time(value) == expected


def test_raw_object_reads_kind_from_key_and_version_from_metadata() -> None:
    obj = raw_object(
        "raw/dwd/daily_kl/recent/tageswerte_KL_01048_akt.zip",
        {"sha256": "abc", "source-last-modified": "28-Oct-2026 09:05:13", "ingest-run-id": "r1"},
    )
    assert (obj.kind, obj.sha256, obj.ingest_run_id) == ("recent", "abc", "r1")
    assert obj.source_modified_at == datetime(2026, 10, 28, 9, 5, 13, tzinfo=UTC)
