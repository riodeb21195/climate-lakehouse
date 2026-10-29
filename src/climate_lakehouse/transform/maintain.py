"""Table maintenance: compaction and snapshot expiry, plus rollback to an earlier snapshot.

`compact` runs after every pipeline run: MERGE batches and daily updates leave small files
behind, and every commit keeps the files of older snapshots alive until they expire.
`rollback` sets a table back to the snapshot that was current at a given time (time travel).
"""

import argparse
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

from climate_lakehouse.config import Settings
from climate_lakehouse.transform.ops import run_job
from climate_lakehouse.transform.quality import GateResult
from climate_lakehouse.transform.spark import CATALOG
from climate_lakehouse.transform.tables import (
    BRONZE_DAILY,
    GOLD_STATION_YEAR,
    SILVER_DAILY,
    SILVER_REJECTED,
    WRITE_ORDER,
    ensure_tables,
)

# Tables that grow with every run; the others are small and replaced as a whole.
COMPACTED = [BRONZE_DAILY, SILVER_DAILY, SILVER_REJECTED, GOLD_STATION_YEAR]
KEEP_SNAPSHOTS = 5
KEEP_DAYS = 7  # snapshots younger than this stay available for rollback


@dataclass
class TableFiles:
    table: str
    files_before: int
    files_after: int
    rewritten: int
    added: int


@dataclass
class MaintainSummary:
    run_id: str
    started_at: datetime
    tables: list[TableFiles] = field(default_factory=list)
    snapshots_expired: int = 0
    checks: list[GateResult] = field(default_factory=list)

    @property
    def rows_written(self) -> int:
        return 0


def data_files(spark: SparkSession, table: str) -> int:
    return spark.table(f"{table}.files").where("content = 0").count()


def compact_table(spark: SparkSession, table: str) -> TableFiles:
    before = data_files(spark, table)
    strategy = "sort" if table in WRITE_ORDER else "binpack"
    result = spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files("
        f"table => '{table}', strategy => '{strategy}', "
        # a partition with two small files is already worth rewriting
        "options => map('min-input-files', '2'))"
    ).first()
    assert result is not None
    rewritten, added = int(result.rewritten_data_files_count), int(result.added_data_files_count)
    return TableFiles(table, before, data_files(spark, table), rewritten, added)


def expire(spark: SparkSession, table: str, now: datetime) -> int:
    older_than = (now - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    row = spark.sql(
        f"CALL {CATALOG}.system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{older_than}', retain_last => {KEEP_SNAPSHOTS})"
    ).first()
    return int(row.deleted_data_files_count) if row else 0


def run_maintain(
    spark: SparkSession, settings: Settings, run_id: str | None = None, now: datetime | None = None
) -> MaintainSummary:
    summary = MaintainSummary(run_id=run_id or str(uuid.uuid4()), started_at=datetime.now(UTC))
    ensure_tables(spark)
    for table in COMPACTED:
        summary.tables.append(compact_table(spark, table))
        summary.snapshots_expired += expire(spark, table, now or summary.started_at)
    return summary


@dataclass
class RollbackSummary:
    run_id: str
    started_at: datetime
    table: str
    to_timestamp: str
    snapshot_before: int
    snapshot_after: int
    checks: list[GateResult] = field(default_factory=list)

    @property
    def rows_written(self) -> int:
        return 0


def rollback(
    spark: SparkSession, table: str, to_timestamp: str, run_id: str | None = None
) -> RollbackSummary:
    """Make the snapshot that was current at `to_timestamp` (UTC) the current one again."""
    started_at = datetime.now(UTC)
    row = spark.sql(
        f"CALL {CATALOG}.system.rollback_to_timestamp("
        f"table => '{table}', timestamp => TIMESTAMP '{to_timestamp}')"
    ).first()
    assert row is not None
    return RollbackSummary(
        run_id=run_id or str(uuid.uuid4()),
        started_at=started_at,
        table=table,
        to_timestamp=to_timestamp,
        snapshot_before=int(row.previous_snapshot_id),
        snapshot_after=int(row.current_snapshot_id),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("compact", help="compact data files and expire old snapshots")
    back = commands.add_parser("rollback", help="roll a table back to an earlier snapshot")
    back.add_argument("table", help="e.g. gold.state_decade")
    back.add_argument("to_timestamp", help="UTC, e.g. '2026-10-29 12:00:00'")
    args = parser.parse_args(argv)

    if args.command == "rollback":
        return run_job(
            "rollback",
            lambda spark, _settings, run_id: rollback(spark, args.table, args.to_timestamp, run_id),
        )
    return run_job(
        "maintain", lambda spark, settings, run_id: run_maintain(spark, settings, run_id)
    )


if __name__ == "__main__":
    sys.exit(main())
