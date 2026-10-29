"""Gold: yearly climate indicators per station, and their decade means per federal state.

gold.station_year is overwritten per decade partition: only decades with silver rows loaded
after the last gold run are recomputed. gold.state_decade is small and depends on every
station-year (reference period), so it is replaced as a whole whenever station_year changes.
"""

import argparse
import logging
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from climate_lakehouse.config import Settings
from climate_lakehouse.transform.ops import run_job
from climate_lakehouse.transform.quality import GateResult, enforce
from climate_lakehouse.transform.tables import (
    GOLD_METRICS,
    GOLD_STATE_DECADE,
    GOLD_STATION_YEAR,
    SILVER_DAILY,
    SILVER_REJECTED,
    SILVER_STATIONS,
    ensure_tables,
    max_timestamp,
    read_property,
    set_property,
)

log = logging.getLogger(__name__)

WATERMARK = "climate.silver-loaded-until"
# A station-year counts for an indicator only if ~90 % of its days have the needed value;
# otherwise gaps would show up as fewer heat or frost days.
MIN_DAYS = 330
REFERENCE_YEARS = (1961, 1990)

# indicator -> (aggregate over the days of a year, silver column it needs). Thresholds are the
# DWD definitions: hot day Tmax >= 30 °C, frost day Tmin < 0 °C, heavy rain >= 20 mm,
# tropical night Tmin >= 20 °C.
INDICATORS = {
    "heat_days": ("count_if(temp_max_c >= 30)", "temp_max_c"),
    "frost_days": ("count_if(temp_min_c < 0)", "temp_min_c"),
    "heavy_rain_days": ("count_if(precipitation_mm >= 20)", "precipitation_mm"),
    "sunshine_h": ("sum(sunshine_h)", "sunshine_h"),
    "temp_mean_c": ("avg(temp_mean_c)", "temp_mean_c"),
    "tropical_nights": ("count_if(temp_min_c >= 20)", "temp_min_c"),
}


@dataclass
class GoldSummary:
    run_id: str
    started_at: datetime
    full_refresh: bool
    finished_at: datetime | None = None
    watermark_from: str | None = None
    watermark_to: str | None = None
    decades_recomputed: list[int] = field(default_factory=list)
    station_years: int = 0
    state_decades: int = 0
    checks: list[GateResult] = field(default_factory=list)

    @property
    def rows_written(self) -> int:
        return self.station_years + self.state_decades


def decade(year: Column) -> Column:
    return (F.floor(year / 10) * 10).cast("int")


def build_station_year(daily: DataFrame, stations: DataFrame) -> DataFrame:
    indicators = [
        F.when(F.count(source) >= MIN_DAYS, F.expr(aggregate)).cast(GOLD_METRICS[name]).alias(name)
        for name, (aggregate, source) in INDICATORS.items()
    ]
    return (
        daily.groupBy("station_id", F.year("measure_date").alias("year"))
        .agg(F.count("*").cast("int").alias("days_observed"), *indicators)
        .join(stations.select("station_id", "state"), "station_id")
    )


def build_state_decade(station_year: DataFrame) -> DataFrame:
    """Decade means per state, and their difference to the 1961-1990 reference.

    The difference is averaged over per-station anomalies (value minus the station's own
    reference mean), so stations opening or closing do not shift a state's result.
    """
    first, last = REFERENCE_YEARS
    by_station = Window.partitionBy("station_id")
    in_reference = F.col("year").between(first, last)
    with_anomaly = station_year.select(
        "state",
        "station_id",
        decade(F.col("year")).alias("decade"),
        *GOLD_METRICS,
        *[
            (F.col(name) - F.avg(F.when(in_reference, F.col(name))).over(by_station)).alias(
                f"{name}_vs_1961_1990"
            )
            for name in GOLD_METRICS
        ],
    )
    return with_anomaly.groupBy("state", "decade").agg(
        F.countDistinct("station_id").cast("int").alias("stations"),
        *[F.avg(name).alias(name) for name in GOLD_METRICS],
        *[F.avg(f"{n}_vs_1961_1990").alias(f"{n}_vs_1961_1990") for n in GOLD_METRICS],
    )


def check_station_year(station_year: DataFrame) -> list[GateResult]:
    duplicates = station_year.groupBy("station_id", "year").count().where("count > 1").count()
    impossible = station_year.where(
        "days_observed > 366 OR heat_days > days_observed OR frost_days > days_observed "
        "OR heavy_rain_days > days_observed OR tropical_nights > days_observed "
        "OR sunshine_h > 17 * days_observed"
    ).count()
    return [
        GateResult("unique_key", duplicates == 0, f"{duplicates} duplicate (station_id, year)"),
        GateResult("indicator_bounds", impossible == 0, f"{impossible} impossible station-years"),
    ]


def run_gold(
    spark: SparkSession, settings: Settings, full_refresh: bool = False, run_id: str | None = None
) -> GoldSummary:
    summary = GoldSummary(
        run_id=run_id or str(uuid.uuid4()), started_at=datetime.now(UTC), full_refresh=full_refresh
    )
    ensure_tables(spark)
    daily = spark.table(SILVER_DAILY)
    # A row moved to the rejected table is gone from daily: its decade changed as well.
    changes = daily.select("measure_date", "loaded_at").unionByName(
        spark.table(SILVER_REJECTED).select("measure_date", "loaded_at")
    )

    since = None if full_refresh else read_property(spark, GOLD_STATION_YEAR, WATERMARK)
    summary.watermark_from = since
    summary.watermark_to = max_timestamp(changes, "loaded_at")
    if summary.watermark_to is None or summary.watermark_to == since:
        log.info("gold up to date", extra={"watermark": since})
        summary.finished_at = datetime.now(UTC)
        return summary

    measure_decade = decade(F.year("measure_date"))
    changed = changes
    if since is not None:
        changed = changes.where(F.col("loaded_at") > F.lit(since).cast("timestamp"))
    rows = changed.select(measure_decade.alias("d")).distinct().collect()
    summary.decades_recomputed = sorted(row.d for row in rows)
    source = (
        daily if since is None else daily.where(measure_decade.isin(summary.decades_recomputed))
    )

    stamp = [F.lit(summary.run_id).alias("gold_run_id"), F.current_timestamp().alias("loaded_at")]
    station_year = (
        build_station_year(source, spark.table(SILVER_STATIONS)).select("*", *stamp).cache()
    )
    try:
        summary.checks = check_station_year(station_year)
        enforce(summary.checks)
        summary.station_years = station_year.count()
        ordered = station_year.select(*spark.table(GOLD_STATION_YEAR).columns)
        if since is None:
            ordered.writeTo(GOLD_STATION_YEAR).overwrite(F.lit(True))
        else:
            ordered.writeTo(GOLD_STATION_YEAR).overwritePartitions()
    finally:
        station_year.unpersist()

    state_decade = build_state_decade(spark.table(GOLD_STATION_YEAR)).select("*", *stamp)
    state_decade.select(*spark.table(GOLD_STATE_DECADE).columns).writeTo(
        GOLD_STATE_DECADE
    ).overwrite(F.lit(True))
    summary.state_decades = spark.table(GOLD_STATE_DECADE).count()

    set_property(spark, GOLD_STATION_YEAR, WATERMARK, summary.watermark_to)
    summary.finished_at = datetime.now(UTC)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full-refresh", action="store_true", help="recompute all decades, e.g. after silver-full"
    )
    args = parser.parse_args(argv)
    return run_job(
        "gold", lambda spark, settings, run_id: run_gold(spark, settings, args.full_refresh, run_id)
    )


if __name__ == "__main__":
    sys.exit(main())
