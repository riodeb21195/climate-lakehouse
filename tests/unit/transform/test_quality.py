from datetime import date

import pytest
from pyspark.sql import DataFrame, SparkSession

from climate_lakehouse.transform.quality import (
    QualityGateError,
    apply_row_rules,
    check_gates,
    enforce,
)

KEYS = ["station_id", "measure_date"]
COLUMNS = (
    "station_id STRING, measure_date DATE, temp_mean_c DOUBLE, temp_max_c DOUBLE, "
    "temp_min_c DOUBLE, temp_min_ground_c DOUBLE, precipitation_mm DOUBLE, sunshine_h DOUBLE, "
    "snow_depth_cm INT, humidity_pct DOUBLE, cloud_cover_okta DOUBLE, "
    "wind_speed_mean_ms DOUBLE, wind_gust_max_ms DOUBLE"
)
GOOD = (1.0, 2.4, 0.0, -0.9, 0.3, 0.0, 2, 86.0, 7.3, 3.0, 9.1)


def _frame(spark: SparkSession, rows: list[tuple[object, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, COLUMNS)


def _failed(spark: SparkSession, values: tuple[object, ...]) -> list[str]:
    row = apply_row_rules(_frame(spark, [("01048", date(1961, 1, 1), *values)])).first()
    assert row is not None
    return list(row.failed_rules)


def test_plausible_row_passes(spark: SparkSession) -> None:
    assert _failed(spark, GOOD) == []


def test_missing_values_pass(spark: SparkSession) -> None:
    assert _failed(spark, (None,) * len(GOOD)) == []


@pytest.mark.parametrize(
    ("index", "value", "rule"),
    [
        (3, -60.0, "temperature_range"),
        (2, 3.0, "temperature_order"),  # Minimum über dem Mittel
        (4, -0.1, "precipitation_range"),
        (5, 18.0, "sunshine_range"),
        (7, 101.0, "humidity_range"),
        (9, 12.0, "wind_range"),  # Mittel über der Böe
    ],
)
def test_breaking_rule_is_named(spark: SparkSession, index: int, value: float, rule: str) -> None:
    values = list(GOOD)
    values[index] = value
    assert _failed(spark, tuple(values)) == [rule]


def test_row_can_break_several_rules(spark: SparkSession) -> None:
    values = list(GOOD)
    values[5], values[8] = 30.0, 9.5
    assert _failed(spark, tuple(values)) == ["sunshine_range", "cloud_cover_range"]


def test_gates_report_duplicates_and_unknown_stations(spark: SparkSession) -> None:
    day = date(1961, 1, 1)
    checked = apply_row_rules(
        _frame(spark, [("01048", day, *GOOD), ("01048", day, *GOOD), ("99999", day, *GOOD)])
    )
    stations = spark.createDataFrame([("01048",)], "station_id STRING")

    results = {r.name: r for r in check_gates(checked, stations, KEYS, max_reject_ratio=0.01)}

    assert results["reject_ratio"].passed
    assert not results["unique_key"].passed
    assert not results["known_station"].passed
    assert "99999" in results["known_station"].detail
    with pytest.raises(QualityGateError, match=r"unique_key.*known_station"):
        enforce(list(results.values()))
