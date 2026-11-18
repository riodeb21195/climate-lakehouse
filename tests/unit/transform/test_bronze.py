import io
import zipfile
from datetime import date, timedelta

import pytest
from pyspark.sql import SparkSession

from climate_lakehouse.config import Settings
from climate_lakehouse.ingest.dwd import FormatError
from climate_lakehouse.ingest.storage import raw_key
from climate_lakehouse.transform.bronze import parse_product, run_bronze
from climate_lakehouse.transform.tables import BRONZE_DAILY, BRONZE_FILES, BRONZE_STATIONS
from tests.conftest import DRESDEN_HIST_ZIP, DRESDEN_RECENT_ZIP, HIST_ZIP, read_fixture
from tests.unit.transform.conftest import FIXTURE_TIME, MemoryRawReader

DRESDEN_RECENT_KEY = raw_key("recent", DRESDEN_RECENT_ZIP)


def _edit_product(content: bytes, old: bytes, new: bytes) -> bytes:
    """Kopiert ein echtes Archiv mit einer ersetzten Bytefolge in der Produktdatei."""
    source = zipfile.ZipFile(io.BytesIO(content))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as target:
        for member in source.namelist():
            body = source.read(member)
            if member.startswith("produkt_klima_tag"):
                assert old in body
                body = body.replace(old, new)
            target.writestr(member, body)
    return out.getvalue()


def test_parse_product_keeps_values_and_missing_markers() -> None:
    rows = parse_product(read_fixture(DRESDEN_HIST_ZIP))

    assert len(rows) == 36
    first = rows.iloc[0]
    assert (first.station_id, first.measure_date) == ("01048", date(1945, 2, 22))
    assert (first.tmk, first.txk, first.tnk, first.rsk) == (2.4, 4.5, -0.9, 0.5)
    assert first.sdk == -999  # Sonnenschein 1945 nicht gemessen: bleibt wie geliefert
    assert str(rows.dtypes["qn_4"]) == "Int32"


def test_parse_product_rejects_non_numeric_value() -> None:
    content = _edit_product(read_fixture(HIST_ZIP), b";   10;", b";   1O;")
    with pytest.raises(ValueError):
        parse_product(content)


def test_loads_all_files_once(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    settings = settings.model_copy(update={"bronze_batch_files": 3})  # 4 Archive: 2 Batches
    first = run_bronze(spark, settings, raw)

    assert (first.files_seen, first.files_loaded, first.files_skipped) == (5, 5, 0)
    assert first.stations_loaded == 10
    daily = spark.table(BRONZE_DAILY)
    assert first.rows_parsed == daily.count()
    assert {r.source_kind: r["count"] for r in daily.groupBy("source_kind").count().collect()} == {
        "historical": 36 + 640,
        "recent": 22 + 550,
    }
    assert spark.table(BRONZE_STATIONS).count() == 10
    assert spark.table(BRONZE_FILES).count() == 5

    gets_before = raw.gets
    second = run_bronze(spark, settings, raw)
    assert (second.files_loaded, second.files_skipped, second.rows_parsed) == (0, 5, 0)
    assert raw.gets == gets_before
    assert spark.table(BRONZE_DAILY).count() == daily.count()


def test_changed_file_updates_only_changed_rows(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    first_run = spark.table(BRONZE_DAILY).select("bronze_run_id").first()[0]  # type: ignore[index]

    # Der DWD korrigiert einen Wert in der nächsten Version der recent-Datei
    body = raw.objects[DRESDEN_RECENT_KEY][0]
    corrected = _edit_product(body, b"1048;20251220;   10;   5.8;", b"1048;20251220;   10;   6.1;")
    raw.put(DRESDEN_RECENT_KEY, corrected, FIXTURE_TIME + timedelta(days=1))
    summary = run_bronze(spark, settings, raw)

    assert (summary.files_loaded, summary.files_skipped) == (1, 4)
    changed = spark.table(BRONZE_DAILY).where(f"bronze_run_id = '{summary.run_id}'").collect()
    assert [(r.station_id, r.measure_date, r.fx) for r in changed] == [
        ("01048", date(2025, 12, 20), 6.1)
    ]
    assert spark.table(BRONZE_DAILY).where(f"bronze_run_id = '{first_run}'").count() > 0


def test_older_file_version_never_overwrites_newer(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)

    stale = _edit_product(
        raw.objects[DRESDEN_RECENT_KEY][0],
        b"1048;20251220;   10;   5.8;",
        b"1048;20251220;   10;   0.1;",
    )
    raw.put(DRESDEN_RECENT_KEY, stale, FIXTURE_TIME - timedelta(days=1))
    run_bronze(spark, settings, raw)

    row = spark.table(BRONZE_DAILY).where(
        "station_id = '01048' AND measure_date = DATE'2025-12-20' AND source_kind = 'recent'"
    )
    assert row.first().fx == 5.8  # type: ignore[union-attr]


def test_invalid_archive_fails_before_anything_is_written(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    raw.put(raw_key("recent", DRESDEN_RECENT_ZIP), b"<html>maintenance</html>")
    settings = settings.model_copy(update={"bronze_batch_files": 10})

    with pytest.raises(FormatError, match="not a readable ZIP"):
        run_bronze(spark, settings, raw)
    assert spark.table(BRONZE_DAILY).count() == 0
    assert spark.table(BRONZE_FILES).where("source_kind != 'stations'").count() == 0
