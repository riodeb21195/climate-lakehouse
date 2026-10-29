# Runbook

Alle Befehle im Projektordner. Zeiten in UTC. Alarme kommen ins Log (`ALERT …`) und mit
`ALERT_CHANNEL=slack` zusätzlich nach Slack.

## Lage prüfen
| Was | Wo |
|---|---|
| DAG-Läufe, Task-Logs | http://localhost:8080 → `climate_pipeline` |
| Läufe aller Jobs (Status, Dauer, Zeilen, Fehler) | `ops.pipeline_runs`, Dashboard-Tabelle „Pipeline-Läufe“ |
| Gates und Abgleiche | `ops.quality_results` (`passed = false`) |
| Dienste und Speicher | `docker compose ps`, `docker stats --no-stream` |

## Ein Task ist rot
| Task | Ursache meist | Vorgehen |
|---|---|---|
| `download` | DWD nicht erreichbar oder Datei kaputt (Schema-Prüfung) | Retries laufen automatisch. Bleibt er rot: Meldung im Log nennt die Datei; nichts wurde gespeichert. Später `make run`. |
| `bronze` | Datei passt nicht zum erwarteten Format | Log nennt den Key. Bereits geladene Batches bleiben, der nächste Lauf macht weiter. |
| `silver` / `gold` | Quality Gate (`QualityGateError`) | `ops.quality_results` zeigt das Gate. Es wurde nichts geschrieben, Grenze unverändert. Ursache in Bronze/Silver prüfen; legitime Änderung einer Regel → Regel anpassen, `make backfill`. |
| `reconcile` | Schichten zählen nicht zusammen | Meist lief ein Job nicht durch. `make run`; bleibt die Differenz, `make backfill`. |
| `maintain` | Kompaktierung/Expiry fehlgeschlagen | Unkritisch für Daten; beim nächsten Lauf wiederholen. |
| beliebig, `exit -9` / `OutOfMemoryError` | Speicher | `docker stats` prüfen, andere Dienste stoppen (`docker compose --profile dashboard stop superset`), Task in der UI neu starten. |

Jeder Job ist idempotent: Neustart in der UI („Clear“) oder `make run` ist immer sicher.

## Rollback (falscher Stand geschrieben)
1. Zeitpunkt vor dem Fehler finden:
   `SELECT started_at, job, status FROM ops.pipeline_runs ORDER BY started_at DESC`
   (z. B. in Superset SQL Lab, Datenbank `lakehouse`, Tabellen unter `lakehouse.<schema>`).
2. Tabelle zurücksetzen, z. B. Gold:
   `make rollback TABLE=gold.state_decade AT="2026-10-29 12:00:00"`
   Der Lauf steht als Job `rollback` mit Snapshot vorher/nachher in `ops.pipeline_runs`.
3. Silver oder `gold.station_year` zurückgesetzt? Deren Grenzen bleiben stehen, deshalb danach
   `make backfill` (Silver und Gold neu aus Bronze).
4. Rückgängig machen: derselbe Befehl mit einem Zeitpunkt nach dem Fehler.

Snapshots älter als 7 Tage sind abgelaufen (ADR 0006); weiter zurück geht es nur über
`make backfill` aus Bronze bzw. `make download` + `make backfill` aus der Raw-Zone.

## Regel- oder Konfigurationsänderung
- `START_YEAR`, Qualitätsregel, neue Kennzahl: `make backfill` (oder `make silver-full gold-full`).
- `STATION_LIMIT` erhöhen: `make run` lädt die zusätzlichen Stationen inkrementell.

## Neustart, Aufräumen
- `make down` / `make up`: Daten bleiben in den Volumes.
- `make clean`: löscht alle Volumes dieses Projekts (Raw, Tabellen, Airflow-Historie) unwiderruflich.
- Platte voll (Docker startet nicht): `docker builder prune` und `uv cache clean`; Docker
  Desktop braucht einige GB frei.
