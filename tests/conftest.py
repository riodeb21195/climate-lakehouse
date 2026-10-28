from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"
DWD = FIXTURES / "dwd"
HIST_ZIP = "tageswerte_KL_15194_20240401_20251231_hist.zip"
RECENT_ZIP = "tageswerte_KL_15194_akt.zip"


def read_fixture(name: str) -> bytes:
    return (DWD / name).read_bytes()


# Dresden-Klotzsche: a main station with all parameters. Real excerpts: 1945 (before
# START_YEAR), 1960/61 and the last weeks of 2025, which the recent file overlaps.
DRESDEN_HIST_ZIP = "tageswerte_KL_01048_19340101_20251231_hist.zip"
DRESDEN_RECENT_ZIP = "tageswerte_KL_01048_akt.zip"
STATION_LIST = "KL_Tageswerte_Beschreibung_Stationen.txt"
