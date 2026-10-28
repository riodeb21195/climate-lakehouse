# Climate Lakehouse – Deutschlands Klima seit 1950

[![CI](https://github.com/riodeb21195/climate-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/riodeb21195/climate-lakehouse/actions/workflows/ci.yml)

**Leitfrage:** Wie hat sich das Klima in Deutschland seit 1950 messbar verändert – Hitzetage,
Frosttage, Starkregentage, Sonnenstunden und Mitteltemperatur, je Bundesland und Jahrzehnt,
im Vergleich zur Referenzperiode 1961–1990?

> In Arbeit: Raw, Bronze und Silver stehen. Gold, Orchestrierung und Dashboard folgen.

## Datenfluss

```
DWD Open Data ──download──▶ Raw (S3, Dateien 1:1) ──bronze──▶ Bronze (Iceberg) ──silver──▶ Silver (Iceberg)
```

| Schicht | Inhalt | Schreibweise |
|---|---|---|
| Raw `raw/dwd/daily_kl/` | ZIP-Archive und Stationsliste, unverändert, mit Quellstand als S3-Metadaten | nur neue/geänderte Dateien |
| `bronze.dwd_daily_kl` | jede Quellzeile, Werte wie geliefert (`-999` = fehlt), mit Herkunft | MERGE je Dateibatch |
| `bronze.dwd_stations` | Stationsliste mit Koordinaten und Bundesland | ersetzt bei neuer Version |
| `bronze.dwd_files` | Protokoll geladener Dateiversionen (Key + SHA-256) | Append |
| `silver.daily_observations` | eine geprüfte Zeile pro Station und Tag ab `START_YEAR`, `NULL` statt `-999` | MERGE je geänderter Station |
| `silver.daily_observations_rejected` | Zeilen, die eine Qualitätsregel verletzen, mit Regelnamen | MERGE je geänderter Station |
| `silver.stations` | Stationsstammdaten | ersetzt bei jedem Silver-Lauf mit Änderungen |

Alle Jobs sind idempotent: Ein Wiederholungslauf liest und schreibt nichts. Warum MERGE statt
Neuaufbau und wie die Qualitäts-Gates greifen: [ADR 0003](docs/adr/0003-inkrementelles-bronze-silver.md).

## Quickstart

Voraussetzungen: Docker (VM mit mindestens ~4,8 GiB RAM), [uv](https://docs.astral.sh/uv/).

```bash
make install    # .venv anlegen, pre-commit-Hooks installieren
make up         # Postgres, SeaweedFS, Lakekeeper starten (legt .env aus .env.example an)
make download   # DWD-Dateien der ausgewählten Stationen in die Raw-Zone laden
make bronze     # neue Raw-Dateien nach Bronze (Spark im Container)
make silver     # Silver aktualisieren, mit Qualitäts-Gates
```

`STATION_LIMIT` in `.env` bestimmt die Stationsauswahl (Standard 50, `0` = alle). Nach einer
Änderung von `START_YEAR` oder einer Qualitätsregel baut `make silver-full` Silver komplett neu.

## Speicherbudget

Der Stack ist auf eine Docker-VM mit ~4,8 GiB ausgelegt; jedes Limit steht in
`docker-compose.yml`.

| Dienst | Limit |
|---|---|
| Postgres | 256 MiB |
| SeaweedFS | 256 MiB |
| Lakekeeper | 128 MiB |
| Migration, Bootstrap (beenden sich nach dem Start) | 128 / 32 MiB |
| Spark-Job (nur während eines Laufs) | 2 GiB, davon 1,5 GiB Driver-Heap |

## Entwicklung

```bash
make lint       # ruff + mypy
make test       # Unit- und Integrationstests mit Coverage (braucht Docker und Java 17+)
```

Die Spark-Unit-Tests laufen lokal gegen einen Iceberg-Katalog im Temp-Verzeichnis und laden die
Iceberg-JARs beim ersten Lauf von Maven. Die Integrationstests starten einen eigenen
Compose-Stack (`climate-lakehouse-it`, zufällige Ports) und führen die Jobs im Spark-Image aus.

## Architekturentscheidungen

- [ADR 0001: SeaweedFS statt MinIO](docs/adr/0001-seaweedfs-statt-minio.md)
- [ADR 0002: Lakekeeper als Iceberg-REST-Katalog](docs/adr/0002-lakekeeper-rest-katalog.md)
- [ADR 0003: Inkrementelles Bronze und Silver mit Iceberg-MERGE](docs/adr/0003-inkrementelles-bronze-silver.md)
