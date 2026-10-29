import time
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from climate_lakehouse.config import Settings
from climate_lakehouse.transform.benchmark import BASELINE, QUERIES, run_benchmark
from climate_lakehouse.transform.bronze import run_bronze
from climate_lakehouse.transform.gold import run_gold
from climate_lakehouse.transform.maintain import data_files, rollback, run_maintain
from climate_lakehouse.transform.silver import run_silver
from climate_lakehouse.transform.tables import (
    BRONZE_DAILY,
    GOLD_STATE_DECADE,
    GOLD_STATION_YEAR,
    SILVER_DAILY,
    ensure_tables,
)
from tests.unit.transform.conftest import MemoryRawReader


def test_compaction_merges_small_files_without_changing_rows(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    # one MERGE commit per file: the recent partition ends up in several small files
    run_bronze(spark, settings.model_copy(update={"bronze_batch_files": 1}), raw)
    rows = spark.table(BRONZE_DAILY).count()
    before = data_files(spark, BRONZE_DAILY)

    summary = run_maintain(spark, settings)

    bronze = next(t for t in summary.tables if t.table == BRONZE_DAILY)
    assert (bronze.files_before, bronze.files_after) == (before, 2)  # one per source kind
    assert spark.table(BRONZE_DAILY).count() == rows
    # a second run finds nothing to rewrite
    again = next(t for t in run_maintain(spark, settings).tables if t.table == BRONZE_DAILY)
    assert (again.rewritten, again.added) == (0, 0)


def test_rollback_restores_the_table_before_a_bad_write(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    run_silver(spark, settings)
    run_gold(spark, settings)
    good = sorted(spark.table(GOLD_STATE_DECADE).select("state", "decade", "heat_days").collect())
    time.sleep(1)  # snapshot timestamps have millisecond precision; keep the two apart
    before_bad_write = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
    time.sleep(1)

    # a faulty run overwrites the table with wrong numbers
    spark.sql(f"UPDATE {GOLD_STATE_DECADE} SET heat_days = heat_days + 100")
    summary = rollback(spark, GOLD_STATE_DECADE, before_bad_write)

    assert summary.snapshot_before != summary.snapshot_after
    restored = spark.table(GOLD_STATE_DECADE).select("state", "decade", "heat_days").collect()
    assert sorted(restored) == good


def test_new_gold_column_is_added_to_an_existing_table(spark: SparkSession) -> None:
    # gold.station_year as created before tropical_nights existed
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")
    spark.sql(
        f"""CREATE TABLE {GOLD_STATION_YEAR} (station_id STRING NOT NULL, state STRING NOT NULL,
        year INT NOT NULL, days_observed INT NOT NULL, heat_days INT, frost_days INT,
        heavy_rain_days INT, sunshine_h DOUBLE, temp_mean_c DOUBLE,
        gold_run_id STRING NOT NULL, loaded_at TIMESTAMP NOT NULL)
        USING iceberg PARTITIONED BY (truncate(10, year))"""
    )
    spark.sql(
        f"INSERT INTO {GOLD_STATION_YEAR} VALUES "
        "('01048', 'Sachsen', 2003, 365, 20, 70, 3, 2000.0, 10.1, 'old-run', current_timestamp())"
    )
    files = data_files(spark, GOLD_STATION_YEAR)

    ensure_tables(spark)

    row = spark.table(GOLD_STATION_YEAR).first()
    assert row is not None
    assert (row.heat_days, row.tropical_nights) == (20, None)
    assert data_files(spark, GOLD_STATION_YEAR) == files  # metadata change only
    assert "tropical_nights_vs_1961_1990" in spark.table(GOLD_STATE_DECADE).columns


def test_benchmark_times_every_query_on_both_layouts(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    run_silver(spark, settings)

    summary = run_benchmark(spark, settings, repeat=1)

    assert summary.rows == spark.table(SILVER_DAILY).count()
    assert {(t.query, t.table) for t in summary.timings} == {
        (q, table) for q in QUERIES for table in (BASELINE, SILVER_DAILY)
    }
    assert all(t.median_s > 0 for t in summary.timings)
    assert not spark.catalog.tableExists(BASELINE)
