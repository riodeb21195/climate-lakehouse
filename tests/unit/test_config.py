import pytest
from pydantic import ValidationError

from climate_lakehouse.config import Settings


def test_defaults_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("STATION_LIMIT", "START_YEAR", "ALERT_CHANNEL", "SLACK_WEBHOOK_URL"):
        monkeypatch.delenv(var, raising=False)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.station_limit == 50
    assert settings.start_year == 1950
    assert settings.alert_channel == "log"


def test_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STATION_LIMIT", "0")
    monkeypatch.setenv("START_YEAR", "1961")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.station_limit == 0
    assert settings.start_year == 1961


def test_slack_channel_requires_webhook(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALERT_CHANNEL", "slack")
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    with pytest.raises(ValidationError, match="SLACK_WEBHOOK_URL"):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_rejects_negative_station_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STATION_LIMIT", "-1")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]
