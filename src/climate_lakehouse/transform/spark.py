"""SparkSession wired to the Iceberg REST catalog (Lakekeeper) and the S3 store (SeaweedFS)."""

import logging
import sys

from pyspark.sql import SparkSession

from climate_lakehouse.config import Settings

CATALOG = "lakehouse"
ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.12.0"
)


def rest_catalog_conf(settings: Settings) -> dict[str, str]:
    prefix = f"spark.sql.catalog.{CATALOG}"
    return {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": settings.catalog_uri,
        f"{prefix}.warehouse": settings.warehouse,
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        # Lakekeeper vends neither credentials nor signed requests (ADR 0002), so the
        # client brings its own S3 access key.
        f"{prefix}.s3.endpoint": settings.s3_endpoint,
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.access-key-id": settings.s3_access_key,
        f"{prefix}.s3.secret-access-key": settings.s3_secret_key,
        f"{prefix}.client.region": "us-east-1",
    }


def build_session(
    app_name: str, settings: Settings, catalog_conf: dict[str, str] | None = None
) -> SparkSession:
    """Create the session; `catalog_conf` replaces the REST catalog (tests use a local one)."""
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": CATALOG,
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "false",
        "spark.driver.memory": settings.spark_driver_memory,
        "spark.pyspark.python": sys.executable,
        "spark.ui.enabled": "false",
        **(catalog_conf if catalog_conf is not None else rest_catalog_conf(settings)),
    }
    if settings.spark_jars:
        conf["spark.jars"] = settings.spark_jars
    else:
        conf["spark.jars.packages"] = ICEBERG_PACKAGES

    builder = SparkSession.builder.appName(app_name).master(settings.spark_master)
    for key, value in conf.items():
        builder = builder.config(key, value)
    # py4j logs every connection at INFO, and after shutdown into a closed stream
    logging.getLogger("py4j").setLevel(logging.WARNING)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
