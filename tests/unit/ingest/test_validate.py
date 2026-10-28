import io
import zipfile

import pytest

from climate_lakehouse.ingest.dwd import FormatError
from climate_lakehouse.ingest.validate import validate_kl_zip
from tests.conftest import HIST_ZIP, RECENT_ZIP, read_fixture


def _rewrite(content: bytes, transform: dict[str, bytes | None]) -> bytes:
    """Copy a real archive, replacing (bytes) or dropping (None) members by name prefix."""
    source = zipfile.ZipFile(io.BytesIO(content))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as target:
        for member in source.namelist():
            prefix = next((p for p in transform if member.startswith(p)), None)
            if prefix is None:
                target.writestr(member, source.read(member))
            elif transform[prefix] is not None:
                target.writestr(member, transform[prefix])  # type: ignore[arg-type]
    return out.getvalue()


@pytest.mark.parametrize("name", [HIST_ZIP, RECENT_ZIP])
def test_real_archives_pass(name: str) -> None:
    validate_kl_zip(name, read_fixture(name), "15194")


def test_rejects_non_zip() -> None:
    with pytest.raises(FormatError, match="not a readable ZIP"):
        validate_kl_zip("x.zip", b"<html>maintenance</html>", "15194")


def test_rejects_truncated_download() -> None:
    content = read_fixture(HIST_ZIP)
    with pytest.raises(FormatError):
        validate_kl_zip(HIST_ZIP, content[: len(content) // 2], "15194")


def test_rejects_archive_of_other_station() -> None:
    with pytest.raises(FormatError, match="another station"):
        validate_kl_zip(HIST_ZIP, read_fixture(HIST_ZIP), "00044")


def test_rejects_missing_product() -> None:
    content = _rewrite(read_fixture(HIST_ZIP), {"produkt_klima_tag": None})
    with pytest.raises(FormatError, match="exactly one produkt_klima_tag"):
        validate_kl_zip(HIST_ZIP, content, "15194")


def test_rejects_missing_geography() -> None:
    content = _rewrite(read_fixture(HIST_ZIP), {"Metadaten_Geographie": None})
    with pytest.raises(FormatError, match="Geographie"):
        validate_kl_zip(HIST_ZIP, content, "15194")


def test_reports_changed_columns() -> None:
    header = b"STATIONS_ID;MESS_DATUM;QN_3;FX;FM;QN_4;RSK;RSKF;SDK;NM;TMK;TXK;TNK;TGK;NEW;eor\r\n"
    content = _rewrite(read_fixture(HIST_ZIP), {"produkt_klima_tag": header})
    expected = r"missing=\['PM', 'SHK_TAG', 'UPM', 'VPM'\], unexpected=\['NEW'\]"
    with pytest.raises(FormatError, match=expected):
        validate_kl_zip(HIST_ZIP, content, "15194")
