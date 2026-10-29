"""Superset settings for the local dashboard. Only what differs from Superset's defaults."""

import os
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Engine

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
# Charts render in the browser; there are no alerts, reports or thumbnails to compute.
FEATURE_FLAGS = {"ALERT_REPORTS": False, "THUMBNAILS": False}


@event.listens_for(Engine, "connect")
def _attach_lakehouse(dbapi_connection: Any, _record: Any) -> None:
    """Give every DuckDB connection access to the lakehouse as catalog `lakehouse`."""
    if type(dbapi_connection).__module__.split(".")[0] not in {"duckdb", "duckdb_engine"}:
        return
    env = os.environ
    dbapi_connection.execute("LOAD iceberg")
    dbapi_connection.execute("LOAD httpfs")
    dbapi_connection.execute(
        f"CREATE OR REPLACE SECRET lakehouse_s3 (TYPE s3, KEY_ID '{env['S3_ACCESS_KEY']}', "
        f"SECRET '{env['S3_SECRET_KEY']}', ENDPOINT 'seaweedfs:8333', URL_STYLE 'path', "
        "USE_SSL false, REGION 'us-east-1')"
    )
    dbapi_connection.execute(
        f"ATTACH IF NOT EXISTS '{env['WAREHOUSE']}' AS lakehouse (TYPE iceberg, "
        "ENDPOINT 'http://lakekeeper:8181/catalog', AUTHORIZATION_TYPE 'none', "
        "ACCESS_DELEGATION_MODE 'none')"
    )
