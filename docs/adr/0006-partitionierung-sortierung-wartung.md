# ADR 0006: Partitionierung, Sortierung und Tabellenwartung

- Status: angenommen
- Datum: 2026-10-29

## Kontext
Die Abfragen der Auswertung schneiden Silver fast immer nach Zeit (ein Jahr, eine Dekade), die
Wartung und Fehlersuche nach Station. Jeder Lauf erzeugt neue Dateien (MERGE in Batches,
Copy-on-Write), und alte Snapshots halten ersetzte Dateien am Leben. Auf dem Laptop zählt außerdem
der Speicher beim Schreiben.

## Entscheidung
| Tabelle | Partition | Sortierung | Grund |
|---|---|---|---|
| `bronze.dwd_daily_kl` | `source_kind` | – | tägliches MERGE schreibt nur die kleine `recent`-Partition um |
| `silver.daily_observations` | `years(measure_date)` | `station_id, measure_date` | Zeitfilter lesen nur ihre Jahre; die Sortierung hält beim Schreiben eine Datei je Task offen |
| `gold.station_year` | `truncate(10, year)` | – | Überschreiben je Dekade (ADR 0004) |
| übrige | – | – | wenige hundert bis tausend Zeilen |

- **Sortierung statt Fanout:** Ohne Sortierung öffnete das MERGE in Silver pro Task einen
  Parquet-Writer je Jahr (76) und brach mit `OutOfMemoryError: Java heap space` ab (Airflow-Lauf
  vom 2026-10-29, 50 Stationen, 1,5 GiB Heap). Mit `WRITE ORDERED BY` lief derselbe Lauf durch.
- **Wartung als letzter DAG-Task** (`maintain compact`): `rewrite_data_files` (für Silver mit
  Strategie `sort`, sonst `binpack`, ab zwei Dateien je Partition) und `expire_snapshots`
  (älter als 7 Tage, mindestens 5 behalten). Sieben Tage lang ist jeder Stand per
  `make rollback` wiederherstellbar.

## Gemessen (2026-10-29, Intel i5-8210Y, 2 Kerne, Docker-VM 4,8 GiB, 50 Stationen, 1,33 Mio. Zeilen)
`make benchmark`, Median aus 5 Läufen nach einem Aufwärmlauf, Spark im Container:

| Abfrage | unpartitioniert, nach Station sortiert (7 Dateien) | Silver: Jahr-Partition + Station sortiert (77 Dateien) |
|---|---|---|
| ein Jahr, alle Stationen | 1,17 s | 0,70 s |
| eine Station, alle Jahre | 0,68 s | 3,51 s |
| alle Jahre aggregiert | 2,05 s | 2,36 s |

Die Partitionierung halbiert die Jahresabfragen, kostet aber bei Abfragen über eine Station:
dann muss jede Jahresdatei geöffnet werden. Weil Dashboard und Gold nach Zeit lesen, bleibt es
bei der Jahrespartition. Bei 50 Stationen sind die Jahresdateien klein (~18.000 Zeilen); mit
allen Stationen wächst jede Datei, die Zahl bleibt gleich.

## Verworfene Alternativen
- **Partition nach Station (Bucket):** passt zur Wartung, nicht zur Auswertung.
- **Merge-on-Read:** schnellere Updates, aber Delete-Dateien müssen beim Lesen angewendet und
  regelmäßig kompaktiert werden; bei einem täglichen Lauf lohnt das nicht.
- **Kompaktierung nur gelegentlich von Hand:** wird vergessen; als Task kostet sie bei nichts zu
  tun wenige Sekunden.

## Konsequenzen
- Ein Rollback betrifft nur die Daten. Grenzen (`climate.*-loaded-until`) und neue Spalten
  bleiben stehen; nach einem Rollback von Silver ist `make backfill` nötig (Runbook).
- Neue Spalten kommen über `ADDED_COLUMNS` in `transform/tables.py` als reine
  Metadatenänderung dazu (Beispiel: `tropical_nights` in Gold); alte Zeilen sind `NULL` bis
  `make gold-full`.
