"""Abfragezeiten auf Silver (nach Jahr partitioniert, nach Station sortiert) gegen eine Kopie.

Die Vergleichskopie enthält dieselben Daten unpartitioniert in Lieferreihenfolge (Station für
Station, wie die DWD-Dateien ankommen). Jede Abfrage läuft einmal zum Aufwärmen und dann
`--repeat`-mal; es zählt der Median.
"""

import argparse
import statistics
import sys
import time
import uuid
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from climate_lakehouse.config import Settings
from climate_lakehouse.transform.ops import JobSummary, run_job
from climate_lakehouse.transform.tables import SILVER_DAILY

BASELINE = "ops.benchmark_daily_unpartitioned"
QUERIES = {
    "one_year": (
        "SELECT count_if(temp_max_c >= 30) FROM {table} "
        "WHERE measure_date BETWEEN DATE'2003-01-01' AND DATE'2003-12-31'"
    ),
    "one_station": "SELECT year(measure_date), avg(temp_mean_c) FROM {table} "
    "WHERE station_id = '{station}' GROUP BY 1",
    "all_years": "SELECT year(measure_date), count_if(temp_max_c >= 30) FROM {table} GROUP BY 1",
}


@dataclass
class Timing:
    query: str
    table: str
    files: int
    median_s: float
    runs_s: list[float]


@dataclass(kw_only=True)
class BenchmarkSummary(JobSummary):
    rows: int = 0
    timings: list[Timing] = field(default_factory=list)


def _time(spark: SparkSession, sql: str, repeat: int) -> list[float]:
    spark.sql(sql).collect()  # Aufwärmen: JVM, Katalog und S3-Verbindungen
    runs = []
    for _ in range(repeat):
        start = time.perf_counter()
        spark.sql(sql).collect()
        runs.append(round(time.perf_counter() - start, 3))
    return runs


def run_benchmark(
    spark: SparkSession, settings: Settings, repeat: int = 5, run_id: str | None = None
) -> BenchmarkSummary:
    summary = BenchmarkSummary(run_id=run_id or str(uuid.uuid4()))
    spark.sql(f"DROP TABLE IF EXISTS {BASELINE} PURGE")
    spark.sql(
        f"CREATE TABLE {BASELINE} USING iceberg AS "
        f"SELECT * FROM {SILVER_DAILY} ORDER BY station_id, measure_date"
    )
    try:
        summary.rows = spark.table(SILVER_DAILY).count()
        first = spark.table(SILVER_DAILY).select("station_id").orderBy("station_id").first()
        station = first.station_id if first else ""
        for name, template in QUERIES.items():
            for table in (BASELINE, SILVER_DAILY):
                runs = _time(spark, template.format(table=table, station=station), repeat)
                files = spark.table(f"{table}.files").count()
                summary.timings.append(
                    Timing(name, table, files, round(statistics.median(runs), 3), runs)
                )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {BASELINE} PURGE")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeat", type=int, default=5)
    args = parser.parse_args(argv)
    return run_job(
        "benchmark",
        lambda spark, settings, run_id: run_benchmark(spark, settings, args.repeat, run_id),
    )


if __name__ == "__main__":
    sys.exit(main())
