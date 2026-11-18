"""Silver: eine geprüfte Zeile je Station und Tag ab START_YEAR, in lesbaren Einheiten.

Neu berechnet werden nur Stationen mit Bronze-Zeilen, die nach dem letzten Silver-Lauf geladen
wurden. Die Watermark ist eine Tabelleneigenschaft von silver.daily_observations und rückt erst
vor, wenn alle Schreibvorgänge erfolgreich waren; einen gescheiterten Lauf wiederholt also
einfach der nächste.
"""

import argparse
import logging
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from climate_lakehouse.config import Settings
from climate_lakehouse.transform.ops import JobSummary, run_job
from climate_lakehouse.transform.quality import apply_row_rules, check_gates, enforce
from climate_lakehouse.transform.tables import (
    BRONZE_DAILY,
    BRONZE_STATIONS,
    SILVER_DAILY,
    SILVER_MEASURES,
    SILVER_REJECTED,
    SILVER_STATIONS,
    ensure_tables,
    max_timestamp,
    merge_rows,
    read_property,
    row_hash,
    set_property,
)

log = logging.getLogger(__name__)

KEYS = ["station_id", "measure_date"]
MISSING = -999
WATERMARK = "climate.bronze-loaded-until"


@dataclass(kw_only=True)
class SilverSummary(JobSummary):
    full_refresh: bool
    finished_at: datetime | None = None
    watermark_from: str | None = None
    watermark_to: str | None = None
    stations_recomputed: int = 0
    rows_valid: int = 0
    rows_rejected: int = 0

    @property
    def rows_written(self) -> int:
        return self.rows_valid + self.rows_rejected


def build_daily(bronze: DataFrame, start_year: int) -> DataFrame:
    """Führt historical und recent zusammen, übersetzt Fehlwerte und benennt Spalten um.

    Gibt es für einen Tag beide, gewinnt der qualitätsgeprüfte historical-Wert.
    """
    priority = F.when(F.col("source_kind") == "historical", 0).otherwise(1)
    window = Window.partitionBy(*KEYS).orderBy(priority)
    best = (
        bronze.where(F.year("measure_date") >= start_year)
        .withColumn("_rank", F.row_number().over(window))
        .where("_rank = 1")
    )
    measures = [
        F.when(F.col(source) == MISSING, None).otherwise(F.col(source)).alias(target)
        for source, target in SILVER_MEASURES.items()
    ]
    daily = best.select(*KEYS, *measures, "source_kind")
    return daily.withColumn("row_hash", row_hash([*SILVER_MEASURES.values(), "source_kind"]))


def build_stations(bronze_stations: DataFrame) -> DataFrame:
    return bronze_stations.select(
        "station_id",
        "name",
        "state",
        "latitude",
        "longitude",
        "height_m",
        "from_date",
        "to_date",
    )


def run_silver(
    spark: SparkSession, settings: Settings, full_refresh: bool = False, run_id: str | None = None
) -> SilverSummary:
    summary = SilverSummary(run_id=run_id or str(uuid.uuid4()), full_refresh=full_refresh)
    ensure_tables(spark)
    bronze = spark.table(BRONZE_DAILY)

    since = None if full_refresh else read_property(spark, SILVER_DAILY, WATERMARK)
    summary.watermark_from = since
    summary.watermark_to = max_timestamp(bronze, "loaded_at")
    if summary.watermark_to is None or summary.watermark_to == since:
        log.info("silver up to date", extra={"watermark": since})
        summary.finished_at = datetime.now(UTC)
        return summary

    if full_refresh:
        scope = "true"
        source = bronze
        summary.stations_recomputed = bronze.select("station_id").distinct().count()
    else:
        changed = (
            bronze.where(F.col("loaded_at") > F.lit(since).cast("timestamp")) if since else bronze
        )
        stations = sorted(r.station_id for r in changed.select("station_id").distinct().collect())
        summary.stations_recomputed = len(stations)
        scope = "t.station_id IN ({})".format(", ".join(f"'{s}'" for s in stations))
        source = bronze.where(F.col("station_id").isin(stations))

    stations_df = build_stations(spark.table(BRONZE_STATIONS)).cache()
    checked = apply_row_rules(build_daily(source, settings.start_year)).cache()
    try:
        summary.checks = check_gates(checked, stations_df, KEYS, settings.max_reject_ratio)
        log.info("quality gates", extra={"run_id": summary.run_id, "checks": summary.checks})
        enforce(summary.checks)

        stamp = [
            F.lit(summary.run_id).alias("silver_run_id"),
            F.current_timestamp().alias("loaded_at"),
        ]
        valid = checked.where(F.size("failed_rules") == 0).drop("failed_rules").select("*", *stamp)
        rejected = checked.where(F.size("failed_rules") > 0).select("*", *stamp)
        summary.rows_valid = valid.count()
        summary.rows_rejected = rejected.count()

        stations_df.select("*", *stamp).writeTo(SILVER_STATIONS).overwrite(F.lit(True))
        merge_rows(spark, SILVER_DAILY, valid, KEYS, delete_unmatched_when=scope)
        merge_rows(spark, SILVER_REJECTED, rejected, KEYS, delete_unmatched_when=scope)
    finally:
        checked.unpersist()
        stations_df.unpersist()

    set_property(spark, SILVER_DAILY, WATERMARK, summary.watermark_to)
    summary.finished_at = datetime.now(UTC)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="recompute all stations, e.g. after changing START_YEAR or a quality rule",
    )
    args = parser.parse_args(argv)
    return run_job(
        "silver",
        lambda spark, settings, run_id: run_silver(spark, settings, args.full_refresh, run_id),
    )


if __name__ == "__main__":
    sys.exit(main())
