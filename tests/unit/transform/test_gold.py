from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession

from climate_lakehouse.config import Settings
from climate_lakehouse.transform.bronze import run_bronze
from climate_lakehouse.transform.gold import (
    build_state_decade,
    build_station_year,
    check_station_year,
    run_gold,
)
from climate_lakehouse.transform.silver import run_silver
from climate_lakehouse.transform.tables import GOLD_STATE_DECADE, GOLD_STATION_YEAR, SILVER_DAILY
from tests.unit.transform.conftest import MemoryRawReader
from tests.unit.transform.test_silver import FX, _set_recent_value

DAILY = (
    "station_id STRING, measure_date DATE, temp_mean_c DOUBLE, temp_max_c DOUBLE, "
    "temp_min_c DOUBLE, precipitation_mm DOUBLE, sunshine_h DOUBLE"
)
STATION_YEAR = (
    "station_id STRING, state STRING, year INT, days_observed INT, heat_days INT, "
    "frost_days INT, heavy_rain_days INT, sunshine_h DOUBLE, temp_mean_c DOUBLE, "
    "tropical_nights INT"
)


def _days(spark: SparkSession, year: int, count: int) -> DataFrame:
    """`count` Tage ab 1. Januar: Tage 0-9 heiß, 10-14 Frost, 15-17 Starkregen, 18 Tropennacht."""
    rows = []
    for i in range(count):
        tmax = 31.0 if i < 10 else 15.0
        tmin = -1.0 if 10 <= i < 15 else 21.0 if i == 18 else 5.0
        rain = 25.0 if 15 <= i < 18 else 1.0
        rows.append(("00001", date(year, 1, 1) + timedelta(days=i), 10.0, tmax, tmin, rain, 5.0))
    return spark.createDataFrame(rows, DAILY)


def test_station_year_counts_threshold_days(spark: SparkSession) -> None:
    stations = spark.createDataFrame([("00001", "Sachsen")], "station_id STRING, state STRING")
    daily = _days(spark, 2000, 366).unionByName(_days(spark, 2001, 100))

    rows = {r.year: r for r in build_station_year(daily, stations).collect()}

    full = rows[2000]
    assert (full.state, full.days_observed) == ("Sachsen", 366)
    assert (full.heat_days, full.frost_days, full.heavy_rain_days) == (10, 5, 3)
    assert full.tropical_nights == 1
    assert (full.sunshine_h, full.temp_mean_c) == (5.0 * 366, 10.0)
    # 100 beobachtete Tage sind zu wenig: lieber keine Kennzahl als eine verzerrte
    partial = rows[2001]
    assert partial.days_observed == 100
    assert {partial.heat_days, partial.frost_days, partial.sunshine_h} == {None}


def test_state_decade_compares_each_station_with_its_own_reference(spark: SparkSession) -> None:
    rows = [("A", "Bayern", y, 365, 5, 80, 2, 1700.0, 8.0, 0) for y in range(1961, 1991)]
    rows += [("A", "Bayern", y, 365, 15, 60, 4, 1900.0, 9.5, 2) for y in range(2010, 2020)]
    # B öffnete 2010 an einem wärmeren Ort: hebt das Dekadenmittel, hat aber keine Referenz
    rows += [("B", "Bayern", y, 365, 25, 40, 4, 2000.0, 11.0, 6) for y in range(2010, 2020)]

    result = {
        r.decade: r for r in build_state_decade(spark.createDataFrame(rows, STATION_YEAR)).collect()
    }

    recent = result[2010]
    assert (recent.state, recent.stations, recent.heat_days) == ("Bayern", 2, 20.0)
    assert recent.heat_days_vs_1961_1990 == 10.0  # nur Station A: 15 - 5
    assert recent.temp_mean_c_vs_1961_1990 == 1.5
    assert recent.tropical_nights_vs_1961_1990 == 2.0
    assert result[1980].heat_days_vs_1961_1990 == 0.0


def test_gate_flags_impossible_station_years(spark: SparkSession) -> None:
    rows = [
        ("A", "Bayern", 2000, 365, 400, 0, 0, 0.0, 9.0, 0),
        ("A", "Bayern", 2000, 365, 1, 0, 0, 0.0, 9.0, 0),
    ]
    checks = {c.name: c for c in check_station_year(spark.createDataFrame(rows, STATION_YEAR))}

    assert not checks["unique_key"].passed
    assert not checks["indicator_bounds"].passed
    assert checks["indicator_bounds"].detail == "1 impossible station-years"


def _snapshots(spark: SparkSession, table: str) -> int:
    return spark.sql(f"SELECT * FROM {table}.snapshots").count()


def test_gold_counts_every_silver_day_and_repeats_as_no_op(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    run_silver(spark, settings)
    summary = run_gold(spark, settings)

    assert summary.decades_recomputed == [1960, 2020]
    station_year = spark.table(GOLD_STATION_YEAR)
    assert summary.station_years == station_year.count()
    days = station_year.agg({"days_observed": "sum"}).first()
    assert days is not None
    assert days[0] == spark.table(SILVER_DAILY).count()
    assert spark.table(GOLD_STATE_DECADE).where("state = 'Sachsen'").count() == 2

    snapshots = _snapshots(spark, GOLD_STATION_YEAR)
    assert run_gold(spark, settings).decades_recomputed == []
    assert _snapshots(spark, GOLD_STATION_YEAR) == snapshots


def test_gold_rewrites_only_the_changed_decade(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader
) -> None:
    run_bronze(spark, settings, raw)
    run_silver(spark, settings)
    first = run_gold(spark, settings)

    _set_recent_value(raw, "20260110", FX, " 9.9")
    run_bronze(spark, settings, raw)
    run_silver(spark, settings)
    second = run_gold(spark, settings)

    assert second.decades_recomputed == [2020]
    runs = {
        r.year // 10 * 10: r.gold_run_id
        for r in spark.table(GOLD_STATION_YEAR).select("year", "gold_run_id").collect()
    }
    assert runs == {1960: first.run_id, 2020: second.run_id}
