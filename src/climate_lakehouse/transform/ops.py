"""Run bookkeeping shared by all Spark jobs: one row per run, one row per quality check.

`run_job` is the entry point of every job: it creates the session, records the run in
ops.pipeline_runs and ops.quality_results (also when it fails) and raises the alert.
"""

import json
import logging
import uuid
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any, Protocol

from pyspark.sql import SparkSession

from climate_lakehouse.alerts import send_alert
from climate_lakehouse.config import Settings, get_settings
from climate_lakehouse.logging import configure_logging
from climate_lakehouse.transform.quality import GateResult, QualityGateError
from climate_lakehouse.transform.spark import build_session
from climate_lakehouse.transform.tables import OPS_CHECKS, OPS_RUNS, ensure_tables

log = logging.getLogger(__name__)


class JobSummary(Protocol):
    run_id: str
    checks: list[GateResult]

    @property
    def rows_written(self) -> int: ...


Work = Callable[[SparkSession, Settings, str], JobSummary]


def record_run(
    spark: SparkSession,
    job: str,
    run_id: str,
    started_at: datetime,
    status: str,
    rows_written: int = 0,
    details: dict[str, Any] | None = None,
    error: str | None = None,
    checks: list[GateResult] | None = None,
) -> None:
    finished_at = datetime.now(UTC)
    ensure_tables(spark)
    spark.createDataFrame(
        [
            (
                run_id,
                job,
                status,
                started_at,
                finished_at,
                (finished_at - started_at).total_seconds(),
                rows_written,
                json.dumps(details or {}, default=str),
                error,
            )
        ],
        "run_id STRING, job STRING, status STRING, started_at TIMESTAMP, finished_at TIMESTAMP, "
        "duration_s DOUBLE, rows_written BIGINT, details STRING, error STRING",
    ).writeTo(OPS_RUNS).append()
    if checks:
        spark.createDataFrame(
            [(run_id, job, c.name, c.passed, c.detail, finished_at) for c in checks],
            "run_id STRING, job STRING, check_name STRING, passed BOOLEAN, detail STRING, "
            "checked_at TIMESTAMP",
        ).writeTo(OPS_CHECKS).append()


def run_job(
    job: str,
    work: Work,
    session: Callable[[str, Settings], SparkSession] = build_session,
) -> int:
    """Run one job and record its outcome; returns the process exit code."""
    configure_logging()
    settings = get_settings()
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)
    spark = session(job, settings)
    try:
        try:
            summary = work(spark, settings, run_id)
        except Exception as exc:
            checks = exc.results if isinstance(exc, QualityGateError) else []
            log.exception("%s failed", job, extra={"run_id": run_id})
            send_alert(settings, f"{job} failed (run {run_id})", str(exc))
            try:
                record_run(spark, job, run_id, started_at, "failed", error=str(exc), checks=checks)
            except Exception:
                log.exception("could not record failed run", extra={"run_id": run_id})
            return 1

        # Checks that only report (e.g. reconciliation) fail the run without raising.
        error = "; ".join(f"{c.name}: {c.detail}" for c in summary.checks if not c.passed)
        status = "failed" if error else "succeeded"
        details = asdict(summary)  # type: ignore[call-overload]
        record_run(
            spark,
            job,
            run_id,
            started_at,
            status,
            summary.rows_written,
            details,
            error or None,
            summary.checks,
        )
        log.info("%s finished", job, extra={**details, "status": status})
        if error:
            send_alert(settings, f"{job} failed checks (run {run_id})", error)
            return 1
        return 0
    finally:
        spark.stop()
