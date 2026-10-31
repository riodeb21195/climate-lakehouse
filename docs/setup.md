# Setup und Entwicklung

## Voraussetzungen

- Docker (VM mit ~4,8 GiB RAM, ~15 GB freie Platte)
- [uv](https://docs.astral.sh/uv/)
- Für `make test`: Java 17+

## Starten

```bash
make install     # .venv, pre-commit-Hooks
make up          # Image bauen, alle Dienste starten; legt .env aus .env.example an
                 # Airflow (http://localhost:8080) startet den ersten Lauf sofort
make dashboard   # Superset auf http://localhost:8088 (admin / SUPERSET_ADMIN_PASSWORD)
```

`STATION_LIMIT` in `.env` wählt die Stichprobe (Standard 50, reihum je Bundesland; `0` = alle).
`make help` listet alle Befehle.

## Dashboard einrichten

Die Diagramme legt man einmal in Superset an: Datenbank `duckdb:///:memory:`, je eine Abfrage
aus [`dashboard/`](../dashboard/) als Dataset.

## Entwicklung

```bash
make lint        # ruff + mypy (strict)
make test        # Unit- und Integrationstests mit Coverage (braucht Docker und Java 17+)
make benchmark   # Abfragezeiten Silver vs. unpartitionierte Kopie
```

Einzelne Jobs ohne Airflow: `make download | bronze | silver | gold | reconcile | compact`,
Neuaufbau mit `make silver-full` / `make gold-full`. `make run` löst den DAG aus,
`make backfill` mit `full_refresh`.

## Tests, CI und CD

- Die Unit-Tests nutzen echte, gekürzte DWD-Dateien (`tests/fixtures/`) und lokales Spark mit
  Iceberg-Katalog im Temp-Verzeichnis.
- Die Integrationstests starten einen eigenen Compose-Stack (`climate-lakehouse-it`, zufällige
  Ports), führen die Jobs im Pipeline-Image aus und prüfen dort Import und Aufbau des DAGs.
- Nach grüner CI auf `main` pusht die CD das Image nach `ghcr.io/riodeb21195/climate-lakehouse`.
