"""Iceberg table definitions for bronze and silver, and the MERGE used to write them."""

import uuid

from pyspark.sql import DataFrame, SparkSession

BRONZE_DAILY = "bronze.dwd_daily_kl"
BRONZE_STATIONS = "bronze.dwd_stations"
BRONZE_FILES = "bronze.dwd_files"
SILVER_DAILY = "silver.daily_observations"
SILVER_REJECTED = "silver.daily_observations_rejected"
SILVER_STATIONS = "silver.stations"

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

_PROPS = "TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'copy-on-write')"


def _columns(names: dict[str, str]) -> str:
    return ",\n  ".join(f"{name} {sql_type}" for name, sql_type in names.items())


_silver_measures = {SILVER_MEASURES[c]: t for c, t in BRONZE_MEASURES.items()}

DDL = [
    "CREATE NAMESPACE IF NOT EXISTS bronze",
    "CREATE NAMESPACE IF NOT EXISTS silver",
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
]


def ensure_tables(spark: SparkSession) -> None:
    for statement in DDL:
        spark.sql(statement)


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
