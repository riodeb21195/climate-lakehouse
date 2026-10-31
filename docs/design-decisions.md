# Design-Entscheidungen, Fehlerszenarien und Grenzen

Ablauf und Datenmodell: [Architektur](architecture.md). Betrieb, Fehlerbehebung und Rollback:
[Runbook](runbook.md).

## Architecture Decision Records

- [ADR 0001: SeaweedFS statt MinIO](adr/0001-seaweedfs-statt-minio.md) (MinIO ist archiviert)
- [ADR 0002: Lakekeeper als Iceberg-REST-Katalog](adr/0002-lakekeeper-rest-katalog.md) (Spark schreibt, DuckDB liest)
- [ADR 0003: Inkrementelles Bronze und Silver mit Iceberg-MERGE](adr/0003-inkrementelles-bronze-silver.md), eigene Quality Gates
- [ADR 0004: Gold-Kennzahlen, Referenzperiode und Überschreiben je Dekade](adr/0004-gold-kennzahlen-und-referenzperiode.md)
- [ADR 0005: Airflow mit LocalExecutor, Spark-Jobs im selben Image](adr/0005-airflow-mit-spark-im-scheduler.md)
- [ADR 0006: Partitionierung, Sortierung und Tabellenwartung](adr/0006-partitionierung-sortierung-wartung.md) (mit Benchmark)

## What happens when …

| Fall | Verhalten | Test |
|---|---|---|
| der Download abbricht | Nächster Lauf lädt nur, was fehlt; nichts doppelt | `test_second_run_transfers_nothing`, `test_download_is_idempotent_and_keeps_source_metadata` |
| der DWD eine HTML-Fehlerseite statt ZIP liefert | Datei wird nicht gespeichert, Exit-Code 1, Alarm | `test_invalid_archive_is_not_stored`, `test_main_returns_nonzero_when_files_fail` |
| der DWD einen Wert korrigiert | Nur die geänderte Zeile wird in Bronze, Silver und Gold (eine Dekade) neu geschrieben | `test_changed_file_updates_only_changed_rows`, `test_recomputes_only_changed_station`, `test_gold_rewrites_only_the_changed_decade` |
| eine ältere Dateiversion später ankommt | überschreibt den neueren Stand nicht | `test_older_file_version_never_overwrites_newer` |
| ein Lauf doppelt ausgeführt wird | liest und schreibt nichts, kein neuer Snapshot | `test_loads_all_files_once`, `test_repeated_run_is_a_no_op`, `test_gold_counts_every_silver_day_and_repeats_as_no_op` |
| ein Wert unplausibel ist (45 h Sonne) | Zeile geht mit Regelnamen in die Ablehnungstabelle | `test_implausible_row_moves_to_rejected_table` |
| zu viele Zeilen durchfallen | Gate stoppt Silver, nichts geschrieben, Gold unverändert, Alarm | `test_failed_gate_writes_nothing_and_keeps_watermark`, `test_failed_gate_is_recorded_and_alerted` |
| eine Raw-Datei nicht in Bronze ankommt | `reconcile` schlägt fehl und alarmiert | `test_reconciliation_detects_raw_files_missing_from_bronze` |
| ein falscher Stand geschrieben wurde | `make rollback` setzt die Tabelle per Time Travel zurück | `test_rollback_restores_the_table_before_a_bad_write` |
| eine neue Kennzahl dazukommt | Spalte wird ohne Neuschreiben ergänzt, alte Zeilen `NULL` bis `gold-full` | `test_new_gold_column_is_added_to_an_existing_table` |
| viele kleine Dateien entstehen | `maintain` kompaktiert, Zeilen bleiben gleich | `test_compaction_merges_small_files_without_changing_rows` |
| Slack nicht erreichbar ist | Alarm steht im Log, der Job meldet trotzdem seinen Fehler | `test_alert_failure_is_logged_not_raised` |

## Bewusst weggelassen

| Weggelassen | Grund |
|---|---|
| Stundenwerte | Alle Kennzahlen der Leitfrage lassen sich aus Tageswerten berechnen. |
| OpenLineage/Marquez | Drei Dienste für ~1 GB RAM; die Abfolge steht im DAG, die Herkunft je Zeile in Bronze. |
| Spark History Server | Dauer und Zeilen je Job stehen in `ops.pipeline_runs`. |
| Great Expectations / Soda | Acht Regeln und drei Gates sind in PySpark wenige Zeilen und normal testbar (ADR 0003). |
| Benchmark in der CI | Geteilte Runner schwanken zu stark für einen Schwellenwert; gemessen wird lokal. |
| Dashboard als Code | Die Abfragen liegen in `dashboard/`, die Diagramme entstehen in der Superset-Oberfläche. |
| Spark-Cluster, Celery, Kubernetes | Ein Rechner, ein Job zur Zeit (ADR 0005). |
| Historie der Stationsorte | Die Leitfrage braucht nur das Bundesland, und das wechselt eine Station nicht. |

## Grenzen

- **Stichprobe:** 50 von 236 geeigneten Stationen (Daten seit 1961, aktiv), bis zu vier je
  Land. Starkregen ist lokal und bräuchte deutlich mehr Stationen.
- **Ortswechsel** einer Station (Höhe, Lage) werden nicht korrigiert; die Abweichung je Station
  gegen ihr eigenes Referenzmittel dämpft den Effekt nur.
- **Das laufende Jahr** hat keine Kennzahlen (weniger als 330 Tage).
- **Harte Abbrüche** (OOM-Kill, Exit -9) erreichen die Jobs nicht mehr: Sie stehen in Airflow,
  aber ohne Eintrag in `ops.pipeline_runs` und ohne Alarm des Jobs.
- **Lokal gedacht:** Airflow-UI ohne Login, Passwörter aus `.env`, kein TLS.
- **Lakekeeper ist 0.x** und gepinnt (ADR 0002).
