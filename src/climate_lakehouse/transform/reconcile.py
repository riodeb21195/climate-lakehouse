"""Reconciliation: every layer accounts for all of its source, checked after each pipeline run.

Unlike the quality gates, these checks write nothing and block nothing; a mismatch fails the
run, is recorded in ops.quality_results and raises an alert.
"""

import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from climate_lakehouse.config import Settings
from climate_lakehouse.ingest.storage import RawReader, S3RawStore
from climate_lakehouse.transform.ops import JobSummary, run_job
from climate_lakehouse.transform.quality import GateResult
from climate_lakehouse.transform.tables import (
    BRONZE_DAILY,
    GOLD_STATION_YEAR,
    SILVER_DAILY,
    SILVER_REJECTED,
    ensure_tables,
    loaded_versions,
)


def _compare(name: str, source: int, target: int, what: str) -> GateResult:
    return GateResult(name, source == target, f"source {source} vs target {target} {what}")


def run_reconcile(
    spark: SparkSession, settings: Settings, raw: RawReader, run_id: str | None = None
) -> JobSummary:
    summary = JobSummary(run_id=run_id or str(uuid.uuid4()))
    ensure_tables(spark)

    loaded = loaded_versions(spark)
    objects = raw.list_objects()
    missing = sum((o.key, o.sha256) not in loaded for o in objects)

    bronze_days = (
        spark.table(BRONZE_DAILY)
        .where(F.year("measure_date") >= settings.start_year)
        .select("station_id", "measure_date")
        .distinct()
        .count()
    )
    silver_valid = spark.table(SILVER_DAILY).count()
    silver_rejected = spark.table(SILVER_REJECTED).count()
    gold_days = spark.table(GOLD_STATION_YEAR).agg(F.sum("days_observed")).first()

    summary.checks = [
        _compare("raw_to_bronze", len(objects), len(objects) - missing, "raw file versions"),
        _compare(
            "bronze_to_silver",
            bronze_days,
            silver_valid + silver_rejected,
            "station-days (valid + rejected)",
        ),
        _compare(
            "silver_to_gold",
            silver_valid,
            int(gold_days[0] or 0) if gold_days else 0,
            "station-days",
        ),
    ]
    return summary


def main() -> int:
    def work(spark: SparkSession, settings: Settings, run_id: str) -> JobSummary:
        return run_reconcile(spark, settings, S3RawStore.from_settings(settings), run_id)

    return run_job("reconcile", work)


if __name__ == "__main__":
    sys.exit(main())
