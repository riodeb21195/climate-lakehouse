# Architektur

Überblick und Ergebnis: [README](../README.md). Begründungen: [Design-Entscheidungen](design-decisions.md).

## Ablauf

Der DAG `climate_pipeline` läuft täglich in dieser Reihenfolge, ein Lauf gleichzeitig:

1. **download**: Verzeichnislisten des DWD lesen, nur neue oder geänderte Dateien laden
   (Name, Zeitstempel und Größe gegen die S3-Metadaten). Jede Datei wird vor dem Speichern
   geprüft (ZIP lesbar, richtige Station, erwartete Spalten). Timeout, Rate Limit,
   Retry mit exponentiellem Backoff nur bei Timeouts, 429 und 5xx.
2. **bronze**: jede neue Dateiversion (Key + SHA-256) parsen und per `MERGE` in Batches nach
   `bronze.dwd_daily_kl`; Werte wie geliefert.
3. **silver**: nur Stationen mit neuen Bronze-Zeilen neu berechnen: `historical` vor `recent`,
   `-999` → `NULL`, Zeilenregeln (physikalische Grenzen), Gates je Lauf.
4. **gold**: Kennzahlen je Station und Jahr nur für geänderte Dekaden, Landesmittel je Dekade
   mit Abweichung zu 1961–1990.
5. **reconcile**: Raw → Bronze → Silver → Gold zählen zusammen.
6. **maintain**: Dateien kompaktieren, Snapshots älter als 7 Tage verwerfen.

Jeder Spark-Job (ab bronze) schreibt einen Eintrag nach `ops.pipeline_runs` und seine Prüfungen
nach `ops.quality_results`, auch wenn er scheitert, und alarmiert dann (Log, optional Slack).
Der Download läuft ohne Spark: Er schreibt seine Zusammenfassung ins JSON-Log und alarmiert bei
fehlgeschlagenen Dateien, erscheint aber nicht in `ops.pipeline_runs` und damit nicht im
Dashboard.
`make run` löst den DAG aus, `make backfill` mit `full_refresh` (Silver und Gold neu aus Bronze).

## Datenmodell

| Tabelle | Inhalt | Schlüssel | Schreibweise |
|---|---|---|---|
| Raw `raw/dwd/daily_kl/…` | ZIP-Archive und Stationsliste 1:1, Quellstand als S3-Metadaten | Objekt-Key | nur neue Versionen |
| `bronze.dwd_daily_kl` | jede Quellzeile, Werte wie geliefert, mit Herkunft | Quellart, Station, Tag | MERGE je Dateibatch |
| `bronze.dwd_stations` | Stationsliste mit Koordinaten und Bundesland | Station | ersetzt bei neuer Version |
| `bronze.dwd_files` | geladene Dateiversionen | Key, SHA-256 | Append |
| `silver.daily_observations` | eine geprüfte Zeile pro Station und Tag ab `START_YEAR` | Station, Tag | MERGE je geänderter Station |
| `silver.daily_observations_rejected` | Zeilen mit verletzten Regeln (`failed_rules`) | Station, Tag | MERGE je geänderter Station |
| `silver.stations` | Stammdaten | Station | ersetzt |
| `gold.station_year` | Hitze-, Frost-, Starkregentage, Tropennächte, Sonnenstunden, Mitteltemperatur | Station, Jahr | Überschreiben je Dekade |
| `gold.state_decade` | Mittel und Abweichung zu 1961–1990 je Bundesland und Dekade | Land, Dekade | ersetzt |
| `ops.pipeline_runs` | Lauf, Job, Status, Dauer, Zeilen, Details, Fehler | Lauf-ID | Append |
| `ops.quality_results` | Ergebnis jedes Gates und Abgleichs | Lauf-ID, Prüfung | Append |

Zeitstempel sind UTC (`spark.sql.session.timeZone`), Messdaten sind Kalendertage des DWD.
Eine Kennzahl ist `NULL`, wenn ein Jahr weniger als 330 Tage mit dem nötigen Wert hat.

## Kennzahlen der Pipeline

Lauf von null am 2026-10-29 (leere Volumes, frische `.env`) auf einem MacBook von 2019
(Intel i5-8210Y, 2 Kerne, 8 GB RAM, Docker-VM 4,8 GiB), 50 Stationen:

| Schritt | Dauer | Ergebnis |
|---|---|---|
| `make up` (Image aus Cache, alle Dienste healthy) | 2 min 3 s | Airflow startet den ersten Lauf |
| download | 35 s | 101 Dateien, 35 MB |
| bronze | 12 min 24 s | 1.500.719 Zeilen aus 100 Archiven |
| silver | 3 min 6 s | 1.332.799 gültig, 9 abgelehnt (0,0007 %) |
| gold | 1 min 28 s | 3.678 Stationsjahre, 128 Land-Dekaden |
| reconcile | 1 min 2 s | 3 von 3 Abgleichen gleich |
| maintain | 1 min 12 s | Bronze 4 → 2 Dateien, Ablehnungen 6 → 1 Datei |
| Wiederholung bronze / silver / gold (`make …`) | 62 s / 52 s / 53 s, fast nur Start von Container und Spark | 0 Archive geladen (101 übersprungen), 0 Stationen, 0 Dekaden neu |

Der erste Bronze-Versuch wurde dabei vom Speicherlimit beendet; die Korrektur steht in
[ADR 0005](adr/0005-airflow-mit-spark-im-scheduler.md). Abfragezeiten mit und ohne
Partitionierung: [ADR 0006](adr/0006-partitionierung-sortierung-wartung.md).

## Speicherbudget

Ausgelegt auf eine Docker-VM mit ~4,8 GiB; jedes Limit steht in `docker-compose.yml`.

| Dienst | Limit | gemessene Spitze (2026-10-29) |
|---|---|---|
| Postgres (Airflow- und Lakekeeper-DB) | 256 MiB | 103 MiB |
| SeaweedFS (Upload-Puffer auf 64 MB begrenzt) | 512 MiB | 380 MiB |
| Lakekeeper | 128 MiB | 84 MiB |
| Airflow API-Server (UI, Execution API) | 640 MiB | 301 MiB |
| Airflow DAG-Processor | 384 MiB | 284 MiB |
| Airflow Scheduler inkl. Spark-Driver (1,5 GiB Heap) | 2,5 GiB | am Limit (Page Cache zählt mit) |
| Superset (Profil `dashboard`, nur bei Bedarf) | 1 GiB | 435 MiB im Leerlauf |
