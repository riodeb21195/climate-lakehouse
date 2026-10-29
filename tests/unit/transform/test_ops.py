import json
from datetime import timedelta
from typing import Any

import httpx
import pytest
from pyspark.sql import SparkSession

from climate_lakehouse.alerts import send_alert
from climate_lakehouse.config import Settings
from climate_lakehouse.ingest.storage import raw_key
from climate_lakehouse.transform import ops
from climate_lakehouse.transform.bronze import run_bronze
from climate_lakehouse.transform.gold import run_gold
from climate_lakehouse.transform.reconcile import run_reconcile
from climate_lakehouse.transform.silver import run_silver
from climate_lakehouse.transform.tables import OPS_CHECKS, OPS_RUNS
from tests.conftest import DRESDEN_RECENT_ZIP
from tests.unit.transform.conftest import FIXTURE_TIME, MemoryRawReader
from tests.unit.transform.test_silver import SDK, _set_recent_value


class _SharedSession:
    """Hands the shared test session to run_job, which would otherwise stop it."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def __getattr__(self, name: str) -> Any:
        return getattr(self._spark, name)

    def stop(self) -> None:
        pass


@pytest.fixture
def alerts(monkeypatch: pytest.MonkeyPatch, settings: Settings) -> list[dict[str, Any]]:
    posted: list[dict[str, Any]] = []
    slack = settings.model_copy(
        update={"alert_channel": "slack", "slack_webhook_url": "https://hooks.test/x"}
    )
    monkeypatch.setattr(ops, "get_settings", lambda: slack)

    def post(url: str, json: dict[str, Any], timeout: float) -> httpx.Response:
        posted.append(json)
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr("climate_lakehouse.alerts.httpx.post", post)
    return posted


def _run(spark: SparkSession, job: str, work: ops.Work) -> int:
    return ops.run_job(job, work, session=lambda _job, _settings: _SharedSession(spark))  # type: ignore[arg-type,return-value]


def test_successful_run_is_recorded_with_its_checks(
    spark: SparkSession, raw: MemoryRawReader, alerts: list[dict[str, Any]]
) -> None:
    assert _run(spark, "bronze", lambda s, st, run_id: run_bronze(s, st, raw, run_id)) == 0
    assert _run(spark, "silver", lambda s, st, run_id: run_silver(s, st, False, run_id)) == 0

    runs = {r.job: r for r in spark.table(OPS_RUNS).collect()}
    assert {r.status for r in runs.values()} == {"succeeded"}
    assert runs["bronze"].rows_written == json.loads(runs["bronze"].details)["rows_parsed"]
    checks = spark.table(OPS_CHECKS).where(f"run_id = '{runs['silver'].run_id}'").collect()
    assert {c.check_name for c in checks} == {"reject_ratio", "unique_key", "known_station"}
    assert alerts == []


def test_failed_gate_is_recorded_and_alerted(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader, alerts: list[dict[str, Any]]
) -> None:
    run_bronze(spark, settings, raw)
    run_silver(spark, settings)
    _set_recent_value(raw, "20260103", SDK, "45.000")
    run_bronze(spark, settings, raw)
    strict = settings.model_copy(update={"max_reject_ratio": 0.0})

    exit_code = _run(spark, "silver", lambda s, _st, run_id: run_silver(s, strict, False, run_id))

    assert exit_code == 1
    run = spark.table(OPS_RUNS).first()
    assert run is not None
    assert (run.job, run.status) == ("silver", "failed")
    assert "reject_ratio" in run.error
    failed = spark.table(OPS_CHECKS).where("NOT passed").collect()
    assert [c.check_name for c in failed] == ["reject_ratio"]
    assert len(alerts) == 1
    assert "silver failed" in alerts[0]["text"]


def test_reconciliation_detects_raw_files_missing_from_bronze(
    spark: SparkSession, settings: Settings, raw: MemoryRawReader, alerts: list[dict[str, Any]]
) -> None:
    run_bronze(spark, settings, raw)
    run_silver(spark, settings)
    run_gold(spark, settings)
    assert all(c.passed for c in run_reconcile(spark, settings, raw).checks)

    # a file arrives after bronze ran
    body = raw.objects[raw_key("recent", DRESDEN_RECENT_ZIP)][0]
    late = DRESDEN_RECENT_ZIP.replace("01048", "01049")
    raw.put(raw_key("recent", late), body, FIXTURE_TIME + timedelta(days=1))
    exit_code = _run(spark, "reconcile", lambda s, st, run_id: run_reconcile(s, st, raw, run_id))

    assert exit_code == 1
    failed = spark.table(OPS_CHECKS).where("NOT passed").collect()
    assert [(c.check_name, c.detail) for c in failed] == [
        ("raw_to_bronze", "source 6 vs target 5 raw file versions")
    ]
    assert "reconcile failed checks" in alerts[0]["text"]


def test_alert_failure_is_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    def post(url: str, json: dict[str, Any], timeout: float) -> httpx.Response:
        raise httpx.ConnectError("no route")

    monkeypatch.setattr("climate_lakehouse.alerts.httpx.post", post)
    slack = settings.model_copy(
        update={"alert_channel": "slack", "slack_webhook_url": "https://hooks.test/x"}
    )
    send_alert(slack, "bronze failed", "boom")

    assert [r.getMessage() for r in caplog.records] == ["ALERT bronze failed", "slack alert failed"]
