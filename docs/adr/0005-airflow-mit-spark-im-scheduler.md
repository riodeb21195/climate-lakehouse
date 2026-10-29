# ADR 0005: Airflow mit LocalExecutor, Spark-Jobs im selben Image

- Status: angenommen
- Datum: 2026-10-29

## Kontext
Die fünf Jobs (download, bronze, silver, gold, reconcile) laufen täglich in fester Reihenfolge.
Ein Job darf erst starten, wenn der vorige erfolgreich war, und ein Backfill muss sich ohne
Sonderwege auslösen lassen. Der Speicher reicht für genau einen Spark-Driver zur Zeit (ADR 0002:
Spark muss ohnehin im Docker-Netz laufen).

## Entscheidung
- **Airflow 3.3.2** mit **LocalExecutor**: Der Scheduler startet jeden Task als Unterprozess.
  Dienste: `airflow-api-server` (UI, Execution API), `airflow-scheduler`,
  `airflow-dag-processor`, dazu `airflow-init` für die Migration. Datenbank ist eine eigene DB in
  der vorhandenen Postgres-Instanz.
- **Ein Image** (`docker/Dockerfile`) auf Basis `apache/airflow:3.3.2-python3.12` mit Java 17,
  den Iceberg-JARs und dem Projekt in einem **eigenen virtualenv** (`/opt/climate/.venv`).
  Airflow bringt eigene Versionen von pandas und pyarrow mit; getrennte Umgebungen verhindern
  Konflikte. Dasselbe Image dient als Service `jobs` für `make bronze` usw. und wird von der CD
  nach GHCR gepusht.
- **Tasks sind `BashOperator`-Aufrufe** der Job-Module. Das Ergebnis meldet der Exit-Code; alles
  andere (Lauf-Metadaten, Gates, Alarm) erledigen die Jobs selbst, also auch beim manuellen Lauf.
- `max_active_runs=1` und `PARALLELISM=1`: nie zwei Spark-Driver gleichzeitig.
- Nur `download` hat Retries (Netzwerk). Ein Gate-Fehler ist deterministisch, ein Retry würde
  nur denselben Fehler wiederholen.
- **Backfill** = DAG-Lauf mit Parameter `full_refresh=true` (`make backfill`): Silver und Gold
  werden aus Bronze neu berechnet. Ein klassischer Backfill über Datumsintervalle bringt hier
  nichts, weil die Jobs nicht nach Intervall, sondern nach Wasserstand arbeiten.

## Verworfene Alternativen
- **SparkSubmitOperator + Spark-Cluster:** Master und Worker kosten mehr als 1 GB zusätzlich; im
  Local Mode reicht ein Prozess.
- **DockerOperator / ein Container pro Task:** braucht den Docker-Socket im Scheduler, das ist
  praktisch Root auf dem Host.
- **CeleryExecutor:** Redis und Worker nur für einen Task zur Zeit.
- **Projekt in Airflows Python-Umgebung installieren:** Versionskonflikte (Airflow-Image:
  pandas 3.0.5, pyarrow 25; Projekt: pandas ≥ 3.1, pyarrow ≥ 26).

## Konsequenzen
- Der Scheduler-Container hat ein Limit von 2,5 GiB, weil der Spark-Driver darin läuft. Beim
  Lauf von null (2026-10-29) wurde Bronze mit 50 Archiven je Batch darin per OOM beendet
  (Exit -9): pandas im Driver, JVM und Airflow-Prozesse teilen sich das Limit. Seitdem lädt
  Bronze 20 Archive je Batch (`BRONZE_BATCH_FILES`). Der Abbruch kam vor dem ersten Commit, es
  war also nichts halb geschrieben; der neue Lauf lud alle Archive.
- Das Image ist groß (Airflow + Java + PySpark). Die CI baut es einmal für die Integrationstests,
  die CD baut es mit GitHub-Actions-Cache erneut und pusht es.
- Die UI ist ohne Login (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`), nur für den lokalen Stack gedacht.
