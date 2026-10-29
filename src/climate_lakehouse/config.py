"""Runtime configuration, read from environment variables (and .env)."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    station_limit: int = Field(default=50, ge=0, description="0 means all stations")
    start_year: int = Field(default=1950, ge=1781, le=2100)

    dwd_base_url: str = (
        "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl"
    )
    http_timeout_s: float = Field(default=30.0, gt=0)
    http_max_retries: int = Field(default=5, ge=0)
    http_requests_per_s: float = Field(default=5.0, gt=0)
    download_workers: int = Field(default=4, ge=1, le=16)

    s3_endpoint: str = "http://localhost:8333"
    s3_bucket: str = "lakehouse"
    s3_access_key: str = ""
    s3_secret_key: str = ""

    catalog_uri: str = "http://localhost:8181/catalog"
    warehouse: str = "climate"
    spark_master: str = "local[*]"
    spark_driver_memory: str = "1536m"
    # Comma-separated local jar paths (baked into the Spark image); empty = resolve from Maven
    spark_jars: str = ""
    # 50 was killed (cgroup OOM) on the first load inside the Airflow scheduler (2026-10-29)
    bronze_batch_files: int = Field(default=20, ge=1)
    max_reject_ratio: float = Field(default=0.01, ge=0, le=1)

    alert_channel: Literal["log", "slack"] = "log"
    slack_webhook_url: str = ""

    @model_validator(mode="after")
    def _webhook_required_for_slack(self) -> "Settings":
        if self.alert_channel == "slack" and not self.slack_webhook_url:
            raise ValueError("ALERT_CHANNEL=slack requires SLACK_WEBHOOK_URL")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
