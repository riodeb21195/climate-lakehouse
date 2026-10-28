import json
import logging

import pytest

from climate_lakehouse.logging import configure_logging


def test_emits_one_json_object_per_line_with_extra_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    logging.getLogger("test").info("downloaded", extra={"station_id": 3, "bytes": 891493})

    line = capsys.readouterr().out.strip()
    record = json.loads(line)
    assert record["msg"] == "downloaded"
    assert record["level"] == "INFO"
    assert record["station_id"] == 3
    assert record["bytes"] == 891493
    assert record["ts"].endswith("+00:00")


def test_includes_exception(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("test").exception("failed")

    record = json.loads(capsys.readouterr().out.strip())
    assert "ValueError: boom" in record["exc"]
