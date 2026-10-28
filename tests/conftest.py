from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"
DWD = FIXTURES / "dwd"
HIST_ZIP = "tageswerte_KL_15194_20240401_20251231_hist.zip"
RECENT_ZIP = "tageswerte_KL_15194_akt.zip"


def read_fixture(name: str) -> bytes:
    return (DWD / name).read_bytes()
