# Climate Lakehouse – Deutschlands Klima seit 1950

[![CI](https://github.com/riodeb21195/climate-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/riodeb21195/climate-lakehouse/actions/workflows/ci.yml)

**Leitfrage:** Wie hat sich das Klima in Deutschland seit 1950 messbar verändert – Hitzetage,
Frosttage, Starkregentage, Sonnenstunden und Mitteltemperatur, je Bundesland und Jahrzehnt,
im Vergleich zur Referenzperiode 1961–1990?

> In Arbeit: Pipeline und Orchestrierung stehen. Dashboard und Messungen folgen.

## Datenfluss

```
DWD Open Data ──download──▶ Raw (S3, Dateien 1:1) ──bronze──▶ Bronze ──silver──▶ Silver ──gold──▶ Gold
                                                                      reconcile: Abgleich aller Schichten
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
| `gold.station_year` | Hitze-, Frost-, Starkregentage, Sonnenstunden, Mitteltemperatur je Station und Jahr | Überschreiben je geänderter Dekade |
| `gold.state_decade` | Mittel je Bundesland und Dekade, Abweichung zu 1961–1990 | ersetzt bei Änderungen |
| `ops.pipeline_runs` | ein Eintrag je Joblauf: Status, Dauer, geschriebene Zeilen, Details, Fehler | Append |
| `ops.quality_results` | Ergebnis jedes Gates und jedes Abgleichs je Lauf | Append |

Alle Jobs sind idempotent: Ein Wiederholungslauf liest und schreibt nichts. Warum MERGE statt
Neuaufbau und wie die Qualitäts-Gates greifen: [ADR 0003](docs/adr/0003-inkrementelles-bronze-silver.md);
wie Gold Lücken und das wechselnde Stationsnetz behandelt: [ADR 0004](docs/adr/0004-gold-kennzahlen-und-referenzperiode.md).

Schlägt ein Job fehl (Gate, Abgleich, Ausnahme), steht das in `ops.pipeline_runs` und es geht ein
Alarm raus: immer ins Log, mit `ALERT_CHANNEL=slack` zusätzlich an `SLACK_WEBHOOK_URL`.

## Quickstart

Voraussetzungen: Docker (VM mit mindestens ~4,8 GiB RAM), [uv](https://docs.astral.sh/uv/).

```bash
make install    # .venv anlegen, pre-commit-Hooks installieren
make up         # Image bauen, alle Dienste inkl. Airflow starten (legt .env aus .env.example an)
```

Airflow (http://localhost:8080) startet den DAG `climate_pipeline` sofort einmal und danach
täglich um 06:00 UTC: `download → bronze → silver → gold → reconcile`. `make run` löst ihn
zusätzlich aus, `make backfill` mit `full_refresh` (Silver und Gold neu aus Bronze).

Einzelne Schritte ohne Airflow, z. B. beim Entwickeln:

```bash
make download   # DWD-Dateien der ausgewählten Stationen in die Raw-Zone laden (vom Host)
make bronze     # neue Raw-Dateien nach Bronze (Spark im Container)
make silver     # Silver aktualisieren, mit Qualitäts-Gates
make gold       # Kennzahlen für geänderte Dekaden neu berechnen
make reconcile  # Abgleich Raw → Bronze → Silver → Gold
```

`STATION_LIMIT` in `.env` bestimmt die Stationsauswahl (Standard 50, `0` = alle). Nach einer
Änderung von `START_YEAR` oder einer Qualitätsregel bauen `make silver-full` und danach
`make gold-full` Silver und Gold komplett neu.

## Speicherbudget

Der Stack ist auf eine Docker-VM mit ~4,8 GiB ausgelegt; jedes Limit steht in
`docker-compose.yml`.

| Dienst | Limit |
|---|---|
| Postgres | 256 MiB |
| SeaweedFS (Upload-Puffer auf 64 MB begrenzt; Spitze 380 MiB im Airflow-Lauf, 2026-10-29) | 512 MiB |
| Lakekeeper | 128 MiB |
| Airflow API-Server (UI) | 640 MiB |
| Airflow DAG-Processor | 384 MiB |
| Airflow Scheduler inkl. Spark-Driver des laufenden Tasks (1,5 GiB Heap) | 2,5 GiB |
| Migrationen, Bootstrap (beenden sich nach dem Start) | 512 / 128 / 32 MiB |
| `make bronze` usw. (Service `jobs`, nur während eines Laufs) | 2 GiB |

## Entwicklung

```bash
make lint       # ruff + mypy
make test       # Unit- und Integrationstests mit Coverage (braucht Docker und Java 17+)
```

Die Spark-Unit-Tests laufen lokal gegen einen Iceberg-Katalog im Temp-Verzeichnis und laden die
Iceberg-JARs beim ersten Lauf von Maven. Die Integrationstests starten einen eigenen
Compose-Stack (`climate-lakehouse-it`, zufällige Ports, ohne Airflow), führen die Jobs im
Pipeline-Image aus und prüfen dort Import und Aufbau des DAGs. Nach grüner CI auf `main` pusht
die CD das Image nach `ghcr.io/riodeb21195/climate-lakehouse` (Tags `latest` und Commit-SHA).

## Architekturentscheidungen

- [ADR 0001: SeaweedFS statt MinIO](docs/adr/0001-seaweedfs-statt-minio.md)
- [ADR 0002: Lakekeeper als Iceberg-REST-Katalog](docs/adr/0002-lakekeeper-rest-katalog.md)
- [ADR 0003: Inkrementelles Bronze und Silver mit Iceberg-MERGE](docs/adr/0003-inkrementelles-bronze-silver.md)
- [ADR 0004: Gold-Kennzahlen, Referenzperiode und Überschreiben je Dekade](docs/adr/0004-gold-kennzahlen-und-referenzperiode.md)
- [ADR 0005: Airflow mit LocalExecutor, Spark-Jobs im selben Image](docs/adr/0005-airflow-mit-spark-im-scheduler.md)
