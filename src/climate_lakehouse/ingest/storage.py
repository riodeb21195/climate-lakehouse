"""Raw zone on S3: files are stored byte-for-byte, with their source version as metadata."""

import hashlib
from typing import TYPE_CHECKING, Protocol

import boto3
from botocore.exceptions import ClientError

from climate_lakehouse.ingest.dwd import RemoteFile

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

RAW_PREFIX = "raw/dwd/daily_kl"


class RawStore(Protocol):
    def is_current(self, key: str, remote: RemoteFile) -> bool: ...

    def put(self, key: str, body: bytes, remote: RemoteFile, run_id: str) -> None: ...


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

    def is_current(self, key: str, remote: RemoteFile) -> bool:
        """True if the stored object came from the same source version (Last-Modified + size)."""
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
        # A single PUT is atomic: readers see the old object or the new one, never a partial file.
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


def raw_key(kind: str, name: str) -> str:
    return f"{RAW_PREFIX}/{kind}/{name}"
