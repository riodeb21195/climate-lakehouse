"""Kritischer Pfad des Ingest-Schritts gegen den echten S3-Server des Stacks (SeaweedFS)."""

import hashlib
from datetime import UTC, date, datetime

import pytest

from climate_lakehouse.config import Settings
from climate_lakehouse.ingest.download import run_download
from climate_lakehouse.ingest.http import HttpClient
from climate_lakehouse.ingest.storage import S3RawStore
from tests.conftest import HIST_ZIP, read_fixture
from tests.integration.conftest import env
from tests.unit.ingest.fakes import BASE, dwd_transport

pytestmark = pytest.mark.integration

# Eigener Bucket, damit die Raw-Zone der Job-Tests so bleibt, wie sie sie angelegt haben.
BUCKET = "raw-ingest-test"


@pytest.fixture(scope="module")
def store(s3_endpoint: str) -> S3RawStore:
    store = S3RawStore.connect(s3_endpoint, BUCKET, env("S3_ACCESS_KEY"), env("S3_SECRET_KEY"))
    store._client.create_bucket(Bucket=BUCKET)
    return store


def _run(store: S3RawStore) -> tuple[int, int]:
    settings = Settings(_env_file=None, station_limit=0, dwd_base_url=BASE)  # type: ignore[call-arg]
    http = HttpClient(1, 0, 1000, transport=dwd_transport(), sleep=lambda _: None)
    summary = run_download(settings, http, store, today=date(2026, 10, 28))
    return summary.files_downloaded, summary.files_skipped


def test_download_is_idempotent_and_keeps_source_metadata(store: S3RawStore) -> None:
    assert _run(store) == (3, 0)
    assert _run(store) == (0, 3)

    head = store._client.head_object(Bucket=BUCKET, Key=f"raw/dwd/daily_kl/historical/{HIST_ZIP}")
    body = read_fixture(HIST_ZIP)
    assert head["ContentLength"] == len(body)
    assert head["Metadata"]["sha256"] == hashlib.sha256(body).hexdigest()
    assert head["Metadata"]["source-last-modified"] == "17-Jun-2026 10:15:08"


def test_list_objects_returns_source_versions(store: S3RawStore) -> None:
    _run(store)

    objects = {o.key: o for o in store.list_objects()}

    hist = objects[f"raw/dwd/daily_kl/historical/{HIST_ZIP}"]
    assert hist.kind == "historical"
    assert hist.sha256 == hashlib.sha256(read_fixture(HIST_ZIP)).hexdigest()
    assert hist.source_modified_at == datetime(2026, 6, 17, 10, 15, 8, tzinfo=UTC)
    assert {o.kind for o in objects.values()} == {"historical", "recent", "stations"}
    assert store.get(hist.key) == read_fixture(HIST_ZIP)
