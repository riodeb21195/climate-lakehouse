"""Critical path of the ingest step against a real S3 server (SeaweedFS in a container)."""

import hashlib
from collections.abc import Iterator
from datetime import date

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

from climate_lakehouse.config import Settings
from climate_lakehouse.ingest.download import run_download
from climate_lakehouse.ingest.http import HttpClient
from climate_lakehouse.ingest.storage import S3RawStore
from tests.conftest import HIST_ZIP, read_fixture
from tests.unit.ingest.fakes import BASE, dwd_transport

pytestmark = pytest.mark.integration

BUCKET = "lakehouse"
KEY, SECRET = "test", "test-secret"
SEAWEEDFS_IMAGE = "chrislusf/seaweedfs:4.51"


@pytest.fixture(scope="module")
def store() -> Iterator[S3RawStore]:
    container = (
        DockerContainer(SEAWEEDFS_IMAGE)
        .with_env("AWS_ACCESS_KEY_ID", KEY)
        .with_env("AWS_SECRET_ACCESS_KEY", SECRET)
        .with_env("S3_BUCKET", BUCKET)
        .with_exposed_ports(8333)
        .waiting_for(HttpWaitStrategy(8333, "/healthz").for_status_code(200))
    )
    with container:
        endpoint = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8333)}"
        yield S3RawStore.connect(endpoint, BUCKET, KEY, SECRET)


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
