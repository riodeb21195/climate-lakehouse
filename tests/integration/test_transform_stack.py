"""All Spark jobs in the pipeline image against the real stack (Lakekeeper, SeaweedFS).

The jobs run inside the Docker network because Lakekeeper hands out its own S3 endpoint
(ADR 0002). The stack gets its own compose project, so a running developer stack is untouched.
"""

import json
import subprocess
from collections.abc import Iterator
from pathlib import Path
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

pytestmark = pytest.mark.integration

ROOT = Path(__file__).parents[2]
ENV_FILE = ROOT / ".env.example"  # documented defaults; the test never reads a real .env
COMPOSE = [
    "docker", "compose",
    "--project-name", "climate-lakehouse-it",
    "--env-file", str(ENV_FILE),
    "-f", str(ROOT / "docker-compose.yml"),
    "-f", str(Path(__file__).parent / "compose.override.yml"),
]  # fmt: skip
FILES = {
    "stations": [STATION_LIST],
    "historical": [HIST_ZIP, DRESDEN_HIST_ZIP],
    "recent": [RECENT_ZIP, DRESDEN_RECENT_ZIP],
}


def _env(name: str) -> str:
    line = next(x for x in ENV_FILE.read_text().splitlines() if x.startswith(f"{name}="))
    return line.split("=", 1)[1]


def _compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([*COMPOSE, *args], cwd=ROOT, capture_output=True, text=True, check=False)


@pytest.fixture(scope="module")
def stack() -> Iterator[S3RawStore]:
    build = _compose("--profile", "jobs", "build", "jobs")
    assert build.returncode == 0, build.stderr
    up = _compose("up", "-d", "--wait")
    try:
        assert up.returncode == 0, up.stderr
        host_port = _compose("port", "seaweedfs", "8333").stdout.strip().rsplit(":", 1)[1]
        store = S3RawStore.connect(
            f"http://localhost:{host_port}",
            _env("S3_BUCKET"),
            _env("S3_ACCESS_KEY"),
            _env("S3_SECRET_KEY"),
        )
        for kind, names in FILES.items():
            for name in names:
                remote = RemoteFile(name, f"https://dwd.test/{name}", "28-Oct-2026 09:05:14", 0)
                store.put(raw_key(kind, name), read_fixture(name), remote, "it-run")
        yield store
    finally:
        _compose("--profile", "jobs", "down", "-v", "--remove-orphans")


def _run_job(module: str) -> dict[str, Any]:
    """Run a job in the pipeline image and return its final summary log record."""
    result = _compose("run", "--rm", "--no-deps", "jobs", module)
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
    listing = stack._client.list_objects_v2(Bucket=_env("S3_BUCKET"), Prefix="warehouse/")
    assert any(o["Key"].endswith(".parquet") for o in listing.get("Contents", []))

    assert _run_job("climate_lakehouse.transform.bronze")["files_loaded"] == 0
    assert _run_job("climate_lakehouse.transform.silver")["stations_recomputed"] == 0
    assert _run_job("climate_lakehouse.transform.gold")["decades_recomputed"] == []
