"""Runtime configuration, read from environment variables (and .env)."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    station_limit: int = Field(default=50, ge=0, description="0 means all stations")
    start_year: int = Field(default=1950, ge=1781, le=2100)

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
