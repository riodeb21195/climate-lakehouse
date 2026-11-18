from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"
DWD = FIXTURES / "dwd"
HIST_ZIP = "tageswerte_KL_15194_20240401_20251231_hist.zip"
RECENT_ZIP = "tageswerte_KL_15194_akt.zip"


def read_fixture(name: str) -> bytes:
    return (DWD / name).read_bytes()


# Dresden-Klotzsche: eine Hauptstation mit allen Parametern. Echte Ausschnitte: 1945 (vor
# START_YEAR), 1960/61 und die letzten Wochen 2025, die sich mit der recent-Datei überschneiden.
DRESDEN_HIST_ZIP = "tageswerte_KL_01048_19340101_20251231_hist.zip"
DRESDEN_RECENT_ZIP = "tageswerte_KL_01048_akt.zip"
STATION_LIST = "KL_Tageswerte_Beschreibung_Stationen.txt"
