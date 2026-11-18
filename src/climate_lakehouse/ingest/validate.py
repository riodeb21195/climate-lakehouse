"""Schema-Prüfung heruntergeladener Dateien, bevor etwas gespeichert wird."""

import io
import re
import zipfile

from climate_lakehouse.ingest.dwd import FormatError

KL_COLUMNS = (
    "STATIONS_ID",
    "MESS_DATUM",
    "QN_3",
    "FX",
    "FM",
    "QN_4",
    "RSK",
    "RSKF",
    "SDK",
    "SHK_TAG",
    "NM",
    "VPM",
    "PM",
    "TMK",
    "UPM",
    "TXK",
    "TNK",
    "TGK",
    "eor",
)
PRODUCT_MEMBER = re.compile(r"^produkt_klima_tag_\d{8}_\d{8}_(\d{5})\.txt$")
_GEOGRAPHY = re.compile(r"^Metadaten_Geographie_(\d{5})\.txt$")


def validate_kl_zip(name: str, content: bytes, station_id: str) -> None:
    """Wirft FormatError mit genauer Meldung, wenn das Archiv kein DWD-Tages-KL-Archiv ist."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(content))
        broken = archive.testzip()
    except zipfile.BadZipFile as exc:
        raise FormatError(f"{name}: not a readable ZIP archive ({exc})") from exc
    if broken is not None:
        raise FormatError(f"{name}: CRC error in member {broken}")

    members = archive.namelist()
    products = [m for m in members if PRODUCT_MEMBER.match(m)]
    if len(products) != 1:
        raise FormatError(f"{name}: expected exactly one produkt_klima_tag file, found {products}")
    if PRODUCT_MEMBER.match(products[0]).group(1) != station_id:  # type: ignore[union-attr]
        raise FormatError(f"{name}: product file {products[0]} belongs to another station")
    if not any(_GEOGRAPHY.match(m) for m in members):
        raise FormatError(f"{name}: Metadaten_Geographie file missing")

    with archive.open(products[0]) as product:
        header = product.readline().decode("latin-1")
    columns = tuple(c.strip() for c in header.strip().split(";"))
    if columns != KL_COLUMNS:
        missing = sorted(set(KL_COLUMNS) - set(columns))
        extra = sorted(set(columns) - set(KL_COLUMNS))
        raise FormatError(
            f"{name}: column layout changed (missing={missing}, unexpected={extra}, "
            f"order_changed={not missing and not extra})"
        )
