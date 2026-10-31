"""All Spark jobs in the pipeline image against the real stack (Lakekeeper, SeaweedFS)."""

import json
from typing import Any

import pytest

from climate_lakehouse.ingest.dwd import RemoteFile
from climate_lakehouse.ingest.storage import S3RawStore, raw_key
from tests.conftest import (
    DRESDEN_HIST_ZIP,
    DRESDEN_RECENT_ZIP,
    HIST_ZIP,
    RECENT_ZIP,
    STATION_LIST,
    read_fixture,
)
from tests.integration.conftest import compose, env

pytestmark = pytest.mark.integration

FILES = {
    "stations": [STATION_LIST],
    "historical": [HIST_ZIP, DRESDEN_HIST_ZIP],
    "recent": [RECENT_ZIP, DRESDEN_RECENT_ZIP],
}


@pytest.fixture(scope="module")
def stack(s3_endpoint: str) -> S3RawStore:
    """The stack with the fixture files in its raw zone."""
    store = S3RawStore.connect(
        s3_endpoint, env("S3_BUCKET"), env("S3_ACCESS_KEY"), env("S3_SECRET_KEY")
    )
    for kind, names in FILES.items():
        for name in names:
            remote = RemoteFile(name, f"https://dwd.test/{name}", "28-Oct-2026 09:05:14", 0)
            store.put(raw_key(kind, name), read_fixture(name), remote, "it-run")
    return store


def _run_job(module: str) -> dict[str, Any]:
    """Run a job in the pipeline image and return its final summary log record."""
    result = compose("run", "--rm", "--no-deps", "jobs", module)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    records = [json.loads(x) for x in result.stdout.splitlines() if x.startswith("{")]
    return next(r for r in reversed(records) if r["msg"].endswith("finished"))


def test_pipeline_writes_iceberg_tables_through_the_catalog(stack: S3RawStore) -> None:
    bronze = _run_job("climate_lakehouse.transform.bronze")
    assert (bronze["files_loaded"], bronze["stations_loaded"]) == (5, 10)

    silver = _run_job("climate_lakehouse.transform.silver")
    assert silver["stations_recomputed"] == 2
    assert silver["rows_valid"] > 0
    assert all(check["passed"] for check in silver["checks"])

    gold = _run_job("climate_lakehouse.transform.gold")
    assert gold["decades_recomputed"] == [1960, 2020]
    reconcile = _run_job("climate_lakehouse.transform.reconcile")
    assert [c["passed"] for c in reconcile["checks"]] == [True, True, True]

    # Data and metadata files land in the warehouse prefix of the bucket
    listing = stack._client.list_objects_v2(Bucket=env("S3_BUCKET"), Prefix="warehouse/")
    assert any(o["Key"].endswith(".parquet") for o in listing.get("Contents", []))

    assert _run_job("climate_lakehouse.transform.bronze")["files_loaded"] == 0
    assert _run_job("climate_lakehouse.transform.silver")["stations_recomputed"] == 0
    assert _run_job("climate_lakehouse.transform.gold")["decades_recomputed"] == []
