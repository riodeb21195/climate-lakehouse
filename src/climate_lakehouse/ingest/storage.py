"""Raw-Zone auf S3: Dateien werden byte-genau gespeichert, ihr Quellstand als Metadaten."""

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

import boto3
from botocore.exceptions import ClientError

from climate_lakehouse.config import Settings
from climate_lakehouse.ingest.dwd import RemoteFile

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

RAW_PREFIX = "raw/dwd/daily_kl"


@dataclass(frozen=True)
class RawObject:
    key: str
    kind: str  # historical | recent | stations
    sha256: str
    source_modified_at: datetime
    ingest_run_id: str


class RawStore(Protocol):
    def is_current(self, key: str, remote: RemoteFile) -> bool: ...

    def put(self, key: str, body: bytes, remote: RemoteFile, run_id: str) -> None: ...


class RawReader(Protocol):
    def list_objects(self) -> list[RawObject]: ...

    def get(self, key: str) -> bytes: ...


class S3RawStore:
    def __init__(self, client: "S3Client", bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    @classmethod
    def connect(cls, endpoint: str, bucket: str, access_key: str, secret_key: str) -> "S3RawStore":
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )
        return cls(client, bucket)

    @classmethod
    def from_settings(cls, settings: Settings) -> "S3RawStore":
        return cls.connect(
            settings.s3_endpoint, settings.s3_bucket, settings.s3_access_key, settings.s3_secret_key
        )

    def is_current(self, key: str, remote: RemoteFile) -> bool:
        """True, wenn das Objekt vom selben Quellstand stammt (Last-Modified + Größe)."""
        try:
            head = self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        meta = head["Metadata"]
        return meta.get("source-last-modified") == remote.last_modified and meta.get(
            "source-size"
        ) == str(remote.size)

    def put(self, key: str, body: bytes, remote: RemoteFile, run_id: str) -> None:
        # Ein einzelner PUT ist atomar: Leser sehen altes oder neues Objekt, nie eine halbe Datei.
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body,
            Metadata={
                "source-url": remote.url,
                "source-last-modified": remote.last_modified,
                "source-size": str(remote.size),
                "sha256": hashlib.sha256(body).hexdigest(),
                "ingest-run-id": run_id,
            },
        )

    def list_objects(self) -> list[RawObject]:
        objects = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=f"{RAW_PREFIX}/"):
            for item in page.get("Contents", []):
                meta = self._client.head_object(Bucket=self._bucket, Key=item["Key"])["Metadata"]
                objects.append(raw_object(item["Key"], meta))
        return objects

    def get(self, key: str) -> bytes:
        return self._client.get_object(Bucket=self._bucket, Key=key)["Body"].read()


def raw_key(kind: str, name: str) -> str:
    return f"{RAW_PREFIX}/{kind}/{name}"


def raw_object(key: str, metadata: dict[str, str]) -> RawObject:
    """Baut ein RawObject aus den S3-Metadaten, die `S3RawStore.put` schreibt."""
    return RawObject(
        key=key,
        kind=key.removeprefix(f"{RAW_PREFIX}/").split("/", 1)[0],
        sha256=metadata["sha256"],
        source_modified_at=parse_listing_time(metadata["source-last-modified"]),
        ingest_run_id=metadata["ingest-run-id"],
    )


def parse_listing_time(value: str) -> datetime:
    """Liest die Zeit der Verzeichnisliste ("17-Jun-2026 10:15:08", Sekunden optional) als UTC."""
    fmt = "%d-%b-%Y %H:%M:%S" if value.count(":") == 2 else "%d-%b-%Y %H:%M"
    return datetime.strptime(value, fmt).replace(tzinfo=UTC)
