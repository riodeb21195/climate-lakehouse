# ADR 0002: Lakekeeper als Iceberg-REST-Katalog

- Status: angenommen
- Datum: 2026-10-28

## Kontext
Spark schreibt die Iceberg-Tabellen, DuckDB (und darüber Superset) liest sie. Beide Engines
müssen denselben Katalog verwenden. Optionen:

| Option | Bewertung |
|---|---|
| JDBC-Katalog in Postgres | kein zusätzlicher Dienst, aber DuckDB kann keinen JDBC-Katalog lesen |
| `apache/iceberg-rest-fixture` | laut Projekt eine Test-Fixture, nicht für den Betrieb gedacht |
| Nessie / Polaris | JVM-Dienste, deutlich mehr Speicher |
| Lakekeeper v0.14.2 | REST-Katalog in Rust, ca. 25 MB RAM gemessen, Metadaten in Postgres |

## Entscheidung
Lakekeeper `quay.io/lakekeeper/catalog:v0.14.2`, Metadaten in einer eigenen Datenbank der
vorhandenen Postgres-Instanz. Ein Bootstrap-Container (`docker/lakekeeper-bootstrap.sh`) legt
Server und Warehouse idempotent an.

## Erkenntnisse aus dem Test (2026-10-28)
- Standardmäßig aktiviert Lakekeeper **Remote Signing**; PyIceberg verlangt dann `s3fs`.
  Weil SeaweedFS ohnehin kein STS kann und alle Engines feste S3-Zugangsdaten bekommen, sind
  `sts-enabled` und `remote-signing-enabled` im Warehouse deaktiviert.
- Lakekeeper liefert beim Laden einer Tabelle den im Warehouse hinterlegten S3-Endpunkt
  (`http://seaweedfs:8333`) mit, und dieser überschreibt den Endpunkt des Clients. Engines, die
  über den Katalog **schreiben**, laufen deshalb im Docker-Netz (Spark im Airflow-Container).
- DuckDB mit `ACCESS_DELEGATION_MODE 'none'` nutzt sein eigenes S3-Secret und liest auch vom Host
  aus (`localhost:8333`). Geprüft mit DuckDB 2.0.0.

## Konsequenzen
- Integrationstests, die über den Katalog schreiben, starten den Job im Container, nicht im
  Test-Prozess auf dem Host.
- Lakekeeper ist 0.x: Version ist gepinnt, Updates nur nach Test des Bootstrap-Skripts.
