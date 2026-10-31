"""Iceberg table definitions of all layers, and the MERGE used to write them."""

import uuid

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE_DAILY = "bronze.dwd_daily_kl"
BRONZE_STATIONS = "bronze.dwd_stations"
BRONZE_FILES = "bronze.dwd_files"
SILVER_DAILY = "silver.daily_observations"
SILVER_REJECTED = "silver.daily_observations_rejected"
SILVER_STATIONS = "silver.stations"
GOLD_STATION_YEAR = "gold.station_year"
GOLD_STATE_DECADE = "gold.state_decade"
OPS_RUNS = "ops.pipeline_runs"
OPS_CHECKS = "ops.quality_results"

# Source columns of produkt_klima_tag (lower-cased) with their types; values stay as delivered.
BRONZE_MEASURES = {
    "qn_3": "INT",
    "fx": "DOUBLE",
    "fm": "DOUBLE",
    "qn_4": "INT",
    "rsk": "DOUBLE",
    "rskf": "INT",
    "sdk": "DOUBLE",
    "shk_tag": "INT",
    "nm": "DOUBLE",
    "vpm": "DOUBLE",
    "pm": "DOUBLE",
    "tmk": "DOUBLE",
    "upm": "DOUBLE",
    "txk": "DOUBLE",
    "tnk": "DOUBLE",
    "tgk": "DOUBLE",
}

# bronze column -> silver column
SILVER_MEASURES = {
    "qn_3": "qn_wind",
    "fx": "wind_gust_max_ms",
    "fm": "wind_speed_mean_ms",
    "qn_4": "qn_climate",
    "rsk": "precipitation_mm",
    "rskf": "precipitation_form",
    "sdk": "sunshine_h",
    "shk_tag": "snow_depth_cm",
    "nm": "cloud_cover_okta",
    "vpm": "vapour_pressure_hpa",
    "pm": "air_pressure_hpa",
    "tmk": "temp_mean_c",
    "upm": "humidity_pct",
    "txk": "temp_max_c",
    "tnk": "temp_min_c",
    "tgk": "temp_min_ground_c",
}

# Yearly indicators per station; NULL where the year has too few observations for it.
GOLD_METRICS = {
    "heat_days": "INT",
    "frost_days": "INT",
    "heavy_rain_days": "INT",
    "sunshine_h": "DOUBLE",
    "temp_mean_c": "DOUBLE",
    # Added after the first release; existing tables get it via `_add_missing_columns`.
    "tropical_nights": "INT",
}

_PROPS = "TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'copy-on-write')"


def _columns(names: dict[str, str]) -> str:
    return ",\n  ".join(f"{name} {sql_type}" for name, sql_type in names.items())


_silver_measures = {SILVER_MEASURES[c]: t for c, t in BRONZE_MEASURES.items()}

DDL = [
    "CREATE NAMESPACE IF NOT EXISTS bronze",
    "CREATE NAMESPACE IF NOT EXISTS silver",
    "CREATE NAMESPACE IF NOT EXISTS gold",
    "CREATE NAMESPACE IF NOT EXISTS ops",
    # Partitioned by source kind: the daily MERGE of the recent files only rewrites the small
    # `recent` partition, the historical partition changes once a year.
    f"""CREATE TABLE IF NOT EXISTS {BRONZE_DAILY} (
  station_id STRING NOT NULL,
  measure_date DATE NOT NULL,
  {_columns(BRONZE_MEASURES)},
  source_kind STRING NOT NULL,
  source_key STRING NOT NULL,
  source_sha256 STRING NOT NULL,
  source_modified_at TIMESTAMP NOT NULL,
  ingest_run_id STRING NOT NULL,
  row_hash STRING NOT NULL,
  bronze_run_id STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL
) USING iceberg PARTITIONED BY (source_kind) {_PROPS}""",
    f"""CREATE TABLE IF NOT EXISTS {BRONZE_STATIONS} (
  station_id STRING NOT NULL,
  from_date DATE NOT NULL,
  to_date DATE NOT NULL,
  height_m INT NOT NULL,
  latitude DOUBLE NOT NULL,
  longitude DOUBLE NOT NULL,
  name STRING NOT NULL,
  state STRING NOT NULL,
  source_key STRING NOT NULL,
  source_sha256 STRING NOT NULL,
  source_modified_at TIMESTAMP NOT NULL,
  bronze_run_id STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL
) USING iceberg {_PROPS}""",
    # One row per raw object version that bronze has fully loaded.
    f"""CREATE TABLE IF NOT EXISTS {BRONZE_FILES} (
  source_key STRING NOT NULL,
  source_sha256 STRING NOT NULL,
  source_kind STRING NOT NULL,
  source_modified_at TIMESTAMP NOT NULL,
  row_count BIGINT NOT NULL,
  bronze_run_id STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL
) USING iceberg {_PROPS}""",
    # Partitioned by year: daily updates only touch the last one or two years.
    f"""CREATE TABLE IF NOT EXISTS {SILVER_DAILY} (
  station_id STRING NOT NULL,
  measure_date DATE NOT NULL,
  {_columns(_silver_measures)},
  source_kind STRING NOT NULL,
  row_hash STRING NOT NULL,
  silver_run_id STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL
) USING iceberg PARTITIONED BY (years(measure_date)) {_PROPS}""",
    f"""CREATE TABLE IF NOT EXISTS {SILVER_REJECTED} (
  station_id STRING NOT NULL,
  measure_date DATE NOT NULL,
  {_columns(_silver_measures)},
  source_kind STRING NOT NULL,
  failed_rules ARRAY<STRING> NOT NULL,
  row_hash STRING NOT NULL,
  silver_run_id STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL
) USING iceberg {_PROPS}""",
    f"""CREATE TABLE IF NOT EXISTS {SILVER_STATIONS} (
  station_id STRING NOT NULL,
  name STRING NOT NULL,
  state STRING NOT NULL,
  latitude DOUBLE NOT NULL,
  longitude DOUBLE NOT NULL,
  height_m INT NOT NULL,
  from_date DATE NOT NULL,
  to_date DATE NOT NULL,
  silver_run_id STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL
) USING iceberg {_PROPS}""",
    # Partitioned by decade: an update rewrites the decades it touches as a whole, and the
    # table stays at a few files per decade instead of one tiny file per year.
    f"""CREATE TABLE IF NOT EXISTS {GOLD_STATION_YEAR} (
  station_id STRING NOT NULL,
  state STRING NOT NULL,
  year INT NOT NULL,
  days_observed INT NOT NULL,
  {_columns(GOLD_METRICS)},
  gold_run_id STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL
) USING iceberg PARTITIONED BY (truncate(10, year)) {_PROPS}""",
    f"""CREATE TABLE IF NOT EXISTS {GOLD_STATE_DECADE} (
  state STRING NOT NULL,
  decade INT NOT NULL,
  stations INT NOT NULL,
  {_columns(dict.fromkeys(GOLD_METRICS, "DOUBLE"))},
  {_columns({f"{name}_vs_1961_1990": "DOUBLE" for name in GOLD_METRICS})},
  gold_run_id STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL
) USING iceberg {_PROPS}""",
    f"""CREATE TABLE IF NOT EXISTS {OPS_RUNS} (
  run_id STRING NOT NULL,
  job STRING NOT NULL,
  status STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP NOT NULL,
  duration_s DOUBLE NOT NULL,
  rows_written BIGINT NOT NULL,
  details STRING NOT NULL,
  error STRING
) USING iceberg {_PROPS}""",
    f"""CREATE TABLE IF NOT EXISTS {OPS_CHECKS} (
  run_id STRING NOT NULL,
  job STRING NOT NULL,
  check_name STRING NOT NULL,
  passed BOOLEAN NOT NULL,
  detail STRING NOT NULL,
  checked_at TIMESTAMP NOT NULL
) USING iceberg {_PROPS}""",
]


# Spark SQL cannot declare a sort order in CREATE TABLE. Sorted writes keep one open file per
# task: unsorted, the MERGE into the year-partitioned silver table opened a Parquet writer per
# year in every task and ran out of driver heap (2026-10-29, 50 stations, 1.5 GiB heap). Station
# order also lets a query for a few stations skip most row groups.
WRITE_ORDER = {SILVER_DAILY: "station_id, measure_date"}


# Columns added after a table was first created. Iceberg adds a column as a metadata change:
# no data file is rewritten, and old rows read as NULL until they are recomputed.
ADDED_COLUMNS = {
    GOLD_STATION_YEAR: {"tropical_nights": "INT"},
    GOLD_STATE_DECADE: {"tropical_nights": "DOUBLE", "tropical_nights_vs_1961_1990": "DOUBLE"},
}


def ensure_tables(spark: SparkSession) -> None:
    for statement in DDL:
        spark.sql(statement)
    _add_missing_columns(spark)
    for table, order in WRITE_ORDER.items():
        if read_property(spark, table, "sort-order") is None:
            spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY {order}")


def _add_missing_columns(spark: SparkSession) -> None:
    for table, columns in ADDED_COLUMNS.items():
        existing = set(spark.table(table).columns)
        missing = {name: t for name, t in columns.items() if name not in existing}
        if missing:
            # Writers select the target's columns by name, so position does not matter.
            added = ", ".join(f"{name} {sql_type}" for name, sql_type in missing.items())
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({added})")


def merge_rows(
    spark: SparkSession,
    target: str,
    source: DataFrame,
    keys: list[str],
    update_when: str = "t.row_hash <> s.row_hash",
    delete_unmatched_when: str | None = None,
) -> None:
    """Upsert `source` into `target` in one atomic commit.

    Rows are only rewritten when their content hash changed, so a repeated run commits no
    changes. `delete_unmatched_when` limits deletes of target rows that are missing from the
    source to the scope the source was computed for (e.g. the stations it covers).
    """
    view = f"merge_source_{uuid.uuid4().hex}"
    source.select(*spark.table(target).columns).createOrReplaceTempView(view)
    on = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    statement = (
        f"MERGE INTO {target} t USING {view} s ON {on} "
        f"WHEN MATCHED AND {update_when} THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    if delete_unmatched_when:
        statement += f" WHEN NOT MATCHED BY SOURCE AND ({delete_unmatched_when}) THEN DELETE"
    try:
        spark.sql(statement)
    finally:
        spark.catalog.dropTempView(view)


def row_hash(columns: list[str]) -> Column:
    """Content hash of a row, compared by `merge_rows` to skip unchanged rows."""
    values = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    return F.sha2(F.concat_ws("|", *values), 256)


def loaded_versions(spark: SparkSession) -> set[tuple[str, str]]:
    """(key, SHA-256) of every raw object version that bronze has fully loaded."""
    rows = spark.table(BRONZE_FILES).select("source_key", "source_sha256").collect()
    return {(row.source_key, row.source_sha256) for row in rows}


def read_property(spark: SparkSession, table: str, key: str) -> str | None:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").where(F.col("key") == key)
    first = rows.first()
    return first["value"] if first else None


def set_property(spark: SparkSession, table: str, key: str, value: str) -> None:
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{key}' = '{value}')")


def max_timestamp(df: DataFrame, column: str) -> str | None:
    """Latest value of a timestamp column at microsecond precision, usable as a watermark."""
    first = df.select(F.date_format(F.max(column), "yyyy-MM-dd HH:mm:ss.SSSSSS")).first()
    return first[0] if first else None
