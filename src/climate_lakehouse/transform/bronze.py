"""Bronze: every DWD file version from the raw zone, parsed into Iceberg with values as delivered.

A raw object is loaded once per content version (key + SHA-256, recorded in bronze.dwd_files),
so a repeated run reads nothing. Files are loaded in batches; each batch is one MERGE commit,
which makes an interrupted run resumable and a re-loaded batch a no-op.
"""

import io
import logging
import sys
import uuid
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from itertools import batched

import pandas as pd
import pyarrow as pa
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from climate_lakehouse.config import Settings, get_settings
from climate_lakehouse.ingest.dwd import FormatError, parse_station_list, station_id_from_name
from climate_lakehouse.ingest.storage import RawObject, RawReader, S3RawStore
from climate_lakehouse.ingest.validate import PRODUCT_MEMBER, validate_kl_zip
from climate_lakehouse.logging import configure_logging
from climate_lakehouse.transform.spark import build_session
from climate_lakehouse.transform.tables import (
    BRONZE_DAILY,
    BRONZE_FILES,
    BRONZE_MEASURES,
    BRONZE_STATIONS,
    ensure_tables,
    merge_rows,
)

log = logging.getLogger(__name__)

DAILY_KINDS = ("historical", "recent")
DAILY_KEYS = ["source_kind", "station_id", "measure_date"]
_PARSED_SCHEMA = (
    "station_id STRING, measure_date DATE, "
    + ", ".join(f"{name} {sql_type}" for name, sql_type in BRONZE_MEASURES.items())
    + ", source_kind STRING, source_key STRING, source_sha256 STRING, "
    "source_modified_at TIMESTAMP, ingest_run_id STRING"
)


@dataclass
class BronzeSummary:
    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    files_seen: int = 0
    files_skipped: int = 0
    files_loaded: int = 0
    rows_parsed: int = 0
    stations_loaded: int = 0


def parse_product(content: bytes) -> pd.DataFrame:
    """Read the produkt_klima_tag file of a daily-KL archive into typed columns.

    Missing values keep the DWD marker -999; translating them is a silver concern.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        member = next(m for m in archive.namelist() if PRODUCT_MEMBER.match(m))
        with archive.open(member) as product:
            frame = pd.read_csv(product, sep=";", encoding="latin-1", dtype=str)
    frame.columns = [c.strip().lower() for c in frame.columns]
    frame = frame.apply(lambda column: column.str.strip())

    parsed = pd.DataFrame(
        {
            "station_id": frame["stations_id"].str.zfill(5),
            "measure_date": pd.to_datetime(frame["mess_datum"], format="%Y%m%d").dt.date,
        }
    )
    for name, sql_type in BRONZE_MEASURES.items():
        values = pd.to_numeric(frame[name], errors="raise")
        parsed[name] = values.astype("Int32" if sql_type == "INT" else "float64")
    return parsed


def run_bronze(spark: SparkSession, settings: Settings, raw: RawReader) -> BronzeSummary:
    summary = BronzeSummary(run_id=str(uuid.uuid4()), started_at=datetime.now(UTC))
    ensure_tables(spark)

    loaded = {
        (row.source_key, row.source_sha256)
        for row in spark.table(BRONZE_FILES).select("source_key", "source_sha256").collect()
    }
    objects = raw.list_objects()
    pending = [o for o in objects if (o.key, o.sha256) not in loaded]
    summary.files_seen = len(objects)
    summary.files_skipped = len(objects) - len(pending)

    stations = [o for o in pending if o.kind == "stations"]
    if stations:
        newest = max(stations, key=lambda o: o.source_modified_at)
        summary.stations_loaded = _load_stations(spark, raw, newest, summary.run_id)
        _record_files(spark, [(o, 0) for o in stations], summary.run_id)
        summary.files_loaded += len(stations)

    daily = sorted((o for o in pending if o.kind in DAILY_KINDS), key=lambda o: o.key)
    for batch in batched(daily, settings.bronze_batch_files):
        summary.rows_parsed += _load_daily_batch(spark, raw, list(batch), summary.run_id)
        summary.files_loaded += len(batch)
        log.info("bronze batch committed", extra={"files": len(batch), "run_id": summary.run_id})

    summary.finished_at = datetime.now(UTC)
    return summary


def _load_daily_batch(
    spark: SparkSession, raw: RawReader, batch: list[RawObject], run_id: str
) -> int:
    # Parsing an archive takes milliseconds with pandas, so it runs on the driver and reaches
    # Spark through Arrow; Python workers would cost more to start than the parsing itself.
    frames = []
    counts = {}
    for obj in batch:
        content = raw.get(obj.key)
        name = obj.key.rsplit("/", 1)[-1]
        validate_kl_zip(name, content, station_id_from_name(name) or "")
        frame = parse_product(content).assign(
            source_kind=obj.kind,
            source_key=obj.key,
            source_sha256=obj.sha256,
            source_modified_at=obj.source_modified_at,
            ingest_run_id=obj.ingest_run_id,
        )
        frames.append(frame)
        counts[obj.key] = len(frame)

    parsed = (
        # Via a pyarrow Table: pandas 3 string columns break Spark's own pandas conversion.
        spark.createDataFrame(
            pa.Table.from_pandas(pd.concat(frames, ignore_index=True), preserve_index=False),
            _PARSED_SCHEMA,
        )
        .withColumn("row_hash", row_hash(list(BRONZE_MEASURES)))
        .withColumn("bronze_run_id", F.lit(run_id))
        .withColumn("loaded_at", F.current_timestamp())
        .cache()
    )
    try:
        merge_rows(
            spark,
            BRONZE_DAILY,
            _latest_per_key(parsed),
            DAILY_KEYS,
            # An older file version (e.g. a stale copy loaded late) never overwrites a newer one.
            update_when="t.row_hash <> s.row_hash AND s.source_modified_at >= t.source_modified_at",
        )
        _record_files(spark, [(o, counts[o.key]) for o in batch], run_id)
    finally:
        parsed.unpersist()
    return sum(counts.values())


def _latest_per_key(rows: DataFrame) -> DataFrame:
    """Keep one row per key; a batch can hold two versions of the same file (renamed archive)."""
    window = Window.partitionBy(*DAILY_KEYS).orderBy(F.col("source_modified_at").desc())
    return rows.withColumn("_rank", F.row_number().over(window)).where("_rank = 1").drop("_rank")


def row_hash(columns: list[str]) -> Column:
    values = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    return F.sha2(F.concat_ws("|", *values), 256)


def _load_stations(spark: SparkSession, raw: RawReader, obj: RawObject, run_id: str) -> int:
    stations = parse_station_list(raw.get(obj.key).decode("latin-1"))
    if not stations:
        raise FormatError(f"{obj.key}: station list is empty")
    rows = [
        (s.station_id, s.from_date, s.to_date, s.height_m, s.latitude, s.longitude, s.name, s.state)
        for s in stations
    ]
    frame = spark.createDataFrame(
        rows,
        "station_id STRING, from_date DATE, to_date DATE, height_m INT, latitude DOUBLE, "
        "longitude DOUBLE, name STRING, state STRING",
    ).select(
        "*",
        F.lit(obj.key).alias("source_key"),
        F.lit(obj.sha256).alias("source_sha256"),
        F.lit(obj.source_modified_at).cast("timestamp").alias("source_modified_at"),
        F.lit(run_id).alias("bronze_run_id"),
        F.current_timestamp().alias("loaded_at"),
    )
    # The list is small and always complete, so each new version replaces the table.
    frame.writeTo(BRONZE_STATIONS).overwrite(F.lit(True))
    return len(rows)


def _record_files(spark: SparkSession, files: list[tuple[RawObject, int]], run_id: str) -> None:
    rows = [(o.key, o.sha256, o.kind, o.source_modified_at, count) for o, count in files]
    frame = spark.createDataFrame(
        rows,
        "source_key STRING, source_sha256 STRING, source_kind STRING, "
        "source_modified_at TIMESTAMP, row_count BIGINT",
    ).select("*", F.lit(run_id).alias("bronze_run_id"), F.current_timestamp().alias("loaded_at"))
    frame.writeTo(BRONZE_FILES).append()


def main() -> int:
    configure_logging()
    settings = get_settings()
    raw = S3RawStore.connect(
        settings.s3_endpoint, settings.s3_bucket, settings.s3_access_key, settings.s3_secret_key
    )
    spark = build_session("bronze", settings)
    try:
        summary = run_bronze(spark, settings, raw)
    finally:
        spark.stop()
    log.info("bronze finished", extra=asdict(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
