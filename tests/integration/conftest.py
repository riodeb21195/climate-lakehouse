"""Der Compose-Stack, den sich alle Integrationstests teilen.

Er läuft als eigenes Compose-Projekt mit zufälligen Host-Ports, ein laufender Entwickler-Stack
bleibt also unberührt. Die Jobs laufen im Docker-Netz, weil Lakekeeper seinen eigenen
S3-Endpunkt ausgibt (ADR 0002).
"""

import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
ENV_FILE = ROOT / ".env.example"  # dokumentierte Defaults; die Tests lesen nie eine echte .env
COMPOSE = [
    "docker", "compose",
    "--project-name", "climate-lakehouse-it",
    "--env-file", str(ENV_FILE),
    "-f", str(ROOT / "docker-compose.yml"),
    "-f", str(Path(__file__).parent / "compose.override.yml"),
]  # fmt: skip


def env(name: str) -> str:
    line = next(x for x in ENV_FILE.read_text().splitlines() if x.startswith(f"{name}="))
    return line.split("=", 1)[1]


def compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([*COMPOSE, *args], cwd=ROOT, capture_output=True, text=True, check=False)


@pytest.fixture(scope="session")
def s3_endpoint() -> Iterator[str]:
    """Startet den Stack einmal je Testsitzung; liefert die Host-Adresse von SeaweedFS."""
    build = compose("--profile", "jobs", "build", "jobs")
    assert build.returncode == 0, build.stderr
    # Das einmalige Bootstrap läuft separat: `up --wait` meldet sein Ende manchmal als Fehler.
    up = compose("up", "-d", "--wait", "seaweedfs", "lakekeeper")
    try:
        assert up.returncode == 0, up.stderr
        bootstrap = compose("run", "--rm", "lakekeeper-bootstrap")
        assert bootstrap.returncode == 0, bootstrap.stdout + bootstrap.stderr
        host_port = compose("port", "seaweedfs", "8333").stdout.strip().rsplit(":", 1)[1]
        yield f"http://localhost:{host_port}"
    finally:
        compose("--profile", "jobs", "down", "-v", "--remove-orphans")
