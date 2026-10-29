import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from climate_lakehouse.config import Settings
from climate_lakehouse.ingest.storage import RawObject, raw_key
from climate_lakehouse.transform.spark import CATALOG, build_session
from tests.conftest import (
    DRESDEN_HIST_ZIP,
    DRESDEN_RECENT_ZIP,
    HIST_ZIP,
    RECENT_ZIP,
    STATION_LIST,
    read_fixture,
)

FIXTURE_OBJECTS = {
    raw_key("stations", STATION_LIST): STATION_LIST,
    raw_key("historical", HIST_ZIP): HIST_ZIP,
    raw_key("recent", RECENT_ZIP): RECENT_ZIP,
    raw_key("historical", DRESDEN_HIST_ZIP): DRESDEN_HIST_ZIP,
    raw_key("recent", DRESDEN_RECENT_ZIP): DRESDEN_RECENT_ZIP,
}
FIXTURE_TIME = datetime(2026, 10, 28, 9, 5, 14, tzinfo=UTC)


class MemoryRawReader:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, RawObject]] = {}
        self.gets = 0

    def put(self, key: str, body: bytes, modified_at: datetime = FIXTURE_TIME) -> None:
        kind = key.split("/")[3]
        sha = hashlib.sha256(body).hexdigest()
        self.objects[key] = (body, RawObject(key, kind, sha, modified_at, "ingest-run"))

    def list_objects(self) -> list[RawObject]:
        return [obj for _, obj in self.objects.values()]

    def get(self, key: str) -> bytes:
        self.gets += 1
        return self.objects[key][0]


@pytest.fixture
def raw() -> MemoryRawReader:
    reader = MemoryRawReader()
    for key, name in FIXTURE_OBJECTS.items():
        reader.put(key, read_fixture(name))
    return reader


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None, spark_master="local[2]", bronze_batch_files=10)  # type: ignore[call-arg]


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    warehouse: Path = tmp_path_factory.mktemp("warehouse")
    local_catalog = {
        f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG}.type": "hadoop",
        f"spark.sql.catalog.{CATALOG}.warehouse": warehouse.as_uri(),
    }
    settings = Settings(_env_file=None, spark_master="local[2]")  # type: ignore[call-arg]
    session = build_session("tests", settings, local_catalog)
    session.conf.set("spark.sql.shuffle.partitions", "1")  # fixtures are tiny
    yield session
    session.stop()


@pytest.fixture
def spark(spark_session: SparkSession) -> Iterator[SparkSession]:
    """The shared session, with all tables dropped after each test."""
    yield spark_session
    for namespace in ("bronze", "silver", "gold", "ops"):
        if spark_session.catalog.databaseExists(namespace):
            for table in spark_session.sql(f"SHOW TABLES IN {namespace}").collect():
                spark_session.sql(f"DROP TABLE {namespace}.{table.tableName} PURGE")
