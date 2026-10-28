import io
import zipfile
from datetime import date, timedelta

import pytest
from pyspark.sql import SparkSession

from climate_lakehouse.config import Settings
from climate_lakehouse.ingest.storage import raw_key
from climate_lakehouse.transform.bronze import run_bronze
from climate_lakehouse.transform.quality import QualityGateError
from climate_lakehouse.transform.silver import WATERMARK, run_silver
from climate_lakehouse.transform.tables import SILVER_DAILY, SILVER_REJECTED, SILVER_STATIONS
from tests.conftest import DRESDEN_RECENT_ZIP
from tests.unit.transform.conftest import FIXTURE_TIME, MemoryRawReader
from tests.unit.transform.test_bronze import _edit_product

DRESDEN_RECENT_KEY = raw_key("recent", DRESDEN_RECENT_ZIP)
DRESDEN = "station_id = '01048'"
FX, SDK = 3, 8  # field positions in produkt_klima_tag


def _watermark(spark: SparkSession) -> str | None:
    rows = spark.sql(f"SHOW TBLPROPERTIES {SILVER_DAILY}").where(f"key = '{WATERMARK}'").collect()
    return rows[0].value if rows else None


def _set_recent_value(raw: MemoryRawReader, day: str, field: int, value: str) -> None:
    """Publish a new version of the Dresden recent file with one field of one day changed."""
    body = raw.objects[DRESDEN_RECENT_KEY][0]
    archive = zipfile.ZipFile(io.BytesIO(body))
    member = next(m for m in archive.namelist() if m.startswith("produkt_klima_tag"))
    line = next(x for x in archive.read(member).splitlines() if f"1048;{day};".encode() in x)
    fields = line.split(b";")
    fields[field] = value.encode()
    raw.put(
        DRESDEN_RECENT_KEY,
        _edit_product(body, line, b";".join(fields)),
        FIXTURE_TIME + timedelta(days=1),
    )


def test_builds_one_clean_row_per_station_and_day(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    summary = run_silver(spark, settings)

    assert summary.stations_recomputed == 2
    assert summary.rows_rejected == 0
    daily = spark.table(SILVER_DAILY)
    dresden = daily.where(DRESDEN)
    # 1945 is before START_YEAR; 12 days of 1960/61, 17 of Dec 2025, 10 of Jan 2026
    assert dresden.count() == 12 + 17 + 10
    assert dresden.where("year(measure_date) < 1950").count() == 0

    overlap = dresden.where("measure_date BETWEEN DATE'2025-12-20' AND DATE'2025-12-31'")
    assert {r.source_kind for r in overlap.collect()} == {"historical"}
    assert {r.source_kind for r in dresden.where("year(measure_date) = 2026").collect()} == {
        "recent"
    }

    day = dresden.where("measure_date = DATE'1961-01-01'").first()
    assert day is not None
    assert (day.temp_mean_c, day.temp_max_c, day.temp_min_c) == (1.0, 2.4, 0.0)
    assert (day.precipitation_mm, day.sunshine_h, day.snow_depth_cm) == (0.3, 0.0, 2)
    assert day.wind_gust_max_ms is None  # -999 in the source

    assert daily.count() == summary.rows_valid
    assert spark.table(SILVER_STATIONS).where(DRESDEN).first().state == "Sachsen"  # type: ignore[union-attr]
    assert _watermark(spark) == summary.watermark_to


def test_repeated_run_is_a_no_op(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    run_silver(spark, settings)
    snapshots = spark.sql(f"SELECT * FROM {SILVER_DAILY}.snapshots").count()

    summary = run_silver(spark, settings)

    assert summary.stations_recomputed == 0
    assert spark.sql(f"SELECT * FROM {SILVER_DAILY}.snapshots").count() == snapshots


def test_recomputes_only_changed_station(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    first = run_silver(spark, settings)

    _set_recent_value(raw, "20260110", FX, " 9.9")
    run_bronze(spark, settings, raw)
    second = run_silver(spark, settings)

    assert second.stations_recomputed == 1
    changed = spark.table(SILVER_DAILY).where(f"silver_run_id = '{second.run_id}'").collect()
    assert [(r.station_id, r.measure_date) for r in changed] == [("01048", date(2026, 1, 10))]
    assert spark.table(SILVER_DAILY).where(f"silver_run_id = '{first.run_id}'").count() > 0


def test_implausible_row_moves_to_rejected_table(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    run_silver(spark, settings)

    # sunshine of 45 h on 2026-01-02: a decoding error, not weather
    _set_recent_value(raw, "20260102", SDK, "45.000")
    run_bronze(spark, settings, raw)
    # the ratio refers to the recomputed station only: 1 of 39 rows
    summary = run_silver(spark, settings.model_copy(update={"max_reject_ratio": 0.05}))

    assert summary.rows_rejected == 1
    rejected = spark.table(SILVER_REJECTED).collect()
    assert [(r.measure_date, r.failed_rules) for r in rejected] == [
        (date(2026, 1, 2), ["sunshine_range"])
    ]
    assert (
        spark.table(SILVER_DAILY).where(f"{DRESDEN} AND measure_date = DATE'2026-01-02'").count()
        == 0
    )


def test_failed_gate_writes_nothing_and_keeps_watermark(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    first = run_silver(spark, settings)
    rows = spark.table(SILVER_DAILY).count()

    _set_recent_value(raw, "20260103", SDK, "45.000")
    run_bronze(spark, settings, raw)
    strict = settings.model_copy(update={"max_reject_ratio": 0.0})

    with pytest.raises(QualityGateError, match="reject_ratio"):
        run_silver(spark, strict)
    assert spark.table(SILVER_DAILY).count() == rows
    assert spark.table(SILVER_REJECTED).count() == 0
    assert _watermark(spark) == first.watermark_to


def test_full_refresh_drops_rows_before_new_start_year(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    run_silver(spark, settings)

    later = settings.model_copy(update={"start_year": 2000})
    assert run_silver(spark, later).stations_recomputed == 0  # nothing new in bronze
    summary = run_silver(spark, later, full_refresh=True)

    assert summary.stations_recomputed == 2
    assert spark.table(SILVER_DAILY).where("year(measure_date) < 2000").count() == 0
