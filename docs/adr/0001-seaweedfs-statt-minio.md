# ADR 0001: SeaweedFS als S3-Objektspeicher statt MinIO

- Status: angenommen
- Datum: 2026-10-28

## Kontext
Das Lakehouse braucht lokal einen S3-kompatiblen Objektspeicher für Rohdaten und Iceberg-Dateien.
Die ursprüngliche Spezifikation nannte MinIO. Geprüft am 2026-10-28:

- Das Repository `minio/minio` ist auf GitHub archiviert und als „no longer maintained“ markiert.
- `minio/minio` auf Docker Hub hat keine abrufbaren Tags mehr.
- Gepflegte Alternativen: SeaweedFS (Apache-2.0, Release 4.51 vom 2026-10-25),
  Garage (AGPL-3.0, v2.5.0), RustFS (1.0.4), Silo (AGPL-3.0, Fork von MinIO).

## Entscheidung
SeaweedFS `chrislusf/seaweedfs:4.51`, als ein Container im Single-Node-Modus. Zugangsdaten und
Bucket kommen über `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` und `S3_BUCKET`; der Bucket wird
beim Start angelegt, ein zusätzlicher Init-Container entfällt.

## Begründung
- Aktiv gepflegt, permissive Lizenz, seit vielen Jahren im Einsatz.
- Gemessen ca. 110 MB RAM im Leerlauf, passt in das Budget von 256 MB.
- Ein Prozess für S3-API und Speicherung; kein Cluster-Setup nötig.

## Konsequenzen
- Kein STS-Endpunkt: Der Katalog kann keine kurzlebigen Zugangsdaten ausstellen (siehe ADR 0002).
- Kein Webkonsole-Ersatz für die MinIO-Console; zum Ansehen reicht `aws s3 ls --endpoint-url`.
- Wechsel auf AWS S3 oder einen anderen S3-Dienst ändert nur Endpunkt und Zugangsdaten.
