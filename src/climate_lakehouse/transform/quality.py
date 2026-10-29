"""Data quality rules for silver, written in plain PySpark.

Row rules decide per row: a row that breaks one goes to the rejected table together with the
names of the rules it broke. Gates decide per run: if one fails, nothing is written.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class QualityGateError(RuntimeError):
    """A run-level quality gate failed; the run must not publish its result."""

    def __init__(self, results: list["GateResult"]) -> None:
        self.results = results
        failed = [r for r in results if not r.passed]
        super().__init__("; ".join(f"{r.name}: {r.detail}" for r in failed))


@dataclass(frozen=True)
class RowRule:
    name: str
    passes: str  # Spark SQL condition; null (a value is missing) counts as passing


# Limits are generous physical bounds for Germany, not climatological thresholds: they catch
# unit and decoding errors, not extreme weather (records: 41.2 °C, -37.8 °C, 312 mm/day).
ROW_RULES = [
    RowRule(
        "temperature_range",
        "temp_mean_c BETWEEN -50 AND 45 AND temp_max_c BETWEEN -50 AND 45 "
        "AND temp_min_c BETWEEN -50 AND 45 AND temp_min_ground_c BETWEEN -55 AND 45",
    ),
    RowRule("temperature_order", "temp_min_c <= temp_mean_c AND temp_mean_c <= temp_max_c"),
    RowRule("precipitation_range", "precipitation_mm BETWEEN 0 AND 350"),
    RowRule("sunshine_range", "sunshine_h BETWEEN 0 AND 17"),
    RowRule("snow_depth_range", "snow_depth_cm BETWEEN 0 AND 1000"),
    RowRule("humidity_range", "humidity_pct BETWEEN 0 AND 100"),
    RowRule("cloud_cover_range", "cloud_cover_okta BETWEEN 0 AND 8"),
    RowRule(
        "wind_range",
        "wind_speed_mean_ms BETWEEN 0 AND 60 AND wind_gust_max_ms BETWEEN 0 AND 100 "
        "AND wind_speed_mean_ms <= wind_gust_max_ms",
    ),
]


def apply_row_rules(df: DataFrame, rules: list[RowRule] = ROW_RULES) -> DataFrame:
    """Add `failed_rules`: the names of all rules the row breaks (empty if it passes all)."""
    failures = [
        F.when(~F.coalesce(F.expr(rule.passes), F.lit(True)), F.lit(rule.name)) for rule in rules
    ]
    return df.withColumn(
        "failed_rules", F.filter(F.array(*failures), lambda name: name.isNotNull())
    )


@dataclass(frozen=True)
class GateResult:
    name: str
    passed: bool
    detail: str


def check_gates(
    checked: DataFrame, stations: DataFrame, keys: list[str], max_reject_ratio: float
) -> list[GateResult]:
    """Run-level checks on the output of `apply_row_rules`."""
    total = checked.count()
    rejected = checked.where(F.size("failed_rules") > 0).count()
    ratio = rejected / total if total else 0.0

    duplicates = checked.groupBy(*keys).count().where(F.col("count") > 1).count()
    orphans = checked.join(stations, "station_id", "left_anti").select("station_id").distinct()
    orphan_ids = sorted(row.station_id for row in orphans.limit(10).collect())

    return [
        GateResult(
            "reject_ratio",
            ratio <= max_reject_ratio,
            f"{rejected} of {total} rows rejected ({ratio:.2%}, limit {max_reject_ratio:.2%})",
        ),
        GateResult("unique_key", duplicates == 0, f"{duplicates} duplicate {tuple(keys)} keys"),
        GateResult(
            "known_station", not orphan_ids, f"stations missing from metadata: {orphan_ids}"
        ),
    ]


def enforce(results: list[GateResult]) -> None:
    if not all(r.passed for r in results):
        raise QualityGateError(results)
