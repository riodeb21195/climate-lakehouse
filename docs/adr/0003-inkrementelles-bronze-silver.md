# ADR 0003: Inkrementelles Bronze und Silver mit Iceberg-MERGE

- Status: angenommen
- Datum: 2026-10-28

## Kontext
Der DWD ersetzt die `recent`-Dateien täglich, die `historical`-Dateien etwa einmal im Jahr
(neuer Dateiname, weil das Enddatum im Namen steht). Pro Lauf ändert sich also nur ein kleiner
Teil der Daten. Die Jobs müssen wiederholbar sein: Ein abgebrochener Lauf wird einfach erneut
gestartet, ein doppelter Lauf darf nichts verändern.

## Entscheidung
**Bronze** (`bronze.dwd_daily_kl`) enthält jede Quellzeile mit den Werten wie geliefert
(inklusive Fehlwertkennung `-999`) und Herkunft (Raw-Key, SHA-256, Dateistand, Ingest-Lauf).
- `bronze.dwd_files` protokolliert jede vollständig geladene Dateiversion (Key + SHA-256).
  Bereits geladene Versionen werden nicht einmal gelesen.
- Neue Dateien werden in Batches (`BRONZE_BATCH_FILES`) per `MERGE` auf den Schlüssel
  (Quellart, Station, Tag) geschrieben. Eine Zeile wird nur ersetzt, wenn sich ihr Inhalts-Hash
  geändert hat und die Quelle nicht älter ist als der gespeicherte Stand.
- Partitioniert nach Quellart: Der tägliche MERGE schreibt nur die kleine `recent`-Partition um.

**Silver** (`silver.daily_observations`) enthält eine Zeile pro Station und Tag ab `START_YEAR`.
- `historical` (qualitätsgeprüft) hat Vorrang vor `recent`; `-999` wird zu `NULL`.
- Neu berechnet werden nur Stationen mit Bronze-Zeilen, die nach dem letzten Silver-Lauf geladen
  wurden. Die Grenze steht als Tabelleneigenschaft `climate.bronze-loaded-until` an der
  Silver-Tabelle und wird erst nach allen Schreibvorgängen verschoben.
- `MERGE` mit `WHEN NOT MATCHED BY SOURCE … DELETE`, begrenzt auf die neu berechneten
  Stationen: Zeilen, die nicht mehr gültig sind, verschwinden aus Silver.
- Partitioniert nach Jahr: Tägliche Updates betreffen nur die letzten ein bis zwei Jahre.

**Qualitätsregeln** sind eigene PySpark-Regeln (`transform/quality.py`), kein Framework:
- Zeilenregeln (physikalische Grenzen, Tmin ≤ Tmittel ≤ Tmax): Verstöße gehen mit den Namen der
  verletzten Regeln nach `silver.daily_observations_rejected`.
- Gates pro Lauf (Ablehnungsquote ≤ `MAX_REJECT_RATIO`, eindeutiger Schlüssel, jede Station in
  den Stammdaten): Schlägt eines fehl, schreibt der Lauf nichts und endet mit Exit-Code 1.

## Verworfene Alternativen
- **Vollständiger Neuaufbau je Lauf** (`INSERT OVERWRITE`): einfach, aber mit allen Stationen
  (1.285 `historical`-Archive, Stand 2026-10-28) würden täglich zig Millionen Zeilen neu
  geschrieben, obwohl sich nur die 573 `recent`-Dateien mit je ~550 Tagen ändern (~0,3 Mio.).
- **Overwrite per Filter** (`writeTo().overwrite(station_id IN …)`): Iceberg löscht dabei nur
  ganze Dateien; Dateien mit Zeilen mehrerer Stationen lassen sich so nicht ersetzen.
- **Append-only Bronze mit Deduplizierung in Silver**: die täglichen `recent`-Kopien würden
  Bronze um ~0,3 Mio. Zeilen pro Tag wachsen lassen, fast alle unverändert.
- **Great Expectations / Soda**: zusätzliche Abhängigkeit und Konfiguration für eine Handvoll
  Regeln, die in PySpark wenige Zeilen sind und sich wie normaler Code testen lassen.

## Konsequenzen
- Jeder Lauf ist idempotent; ein Wiederholungslauf erzeugt keinen Iceberg-Snapshot.
- Bronze, Silver und Ablehnungstabelle werden in getrennten Commits geschrieben. Bricht ein Lauf
  dazwischen ab, bleibt die Grenze stehen und der nächste Lauf rechnet dieselben Stationen neu.
- Ändern sich `START_YEAR` oder eine Regel, ist `make silver-full` nötig.
- Bronze parst die Archive im Driver (pandas, Übergabe an Spark per Arrow). Python-Worker für
  `mapInPandas` kosteten im Test mehr Startzeit als das Parsen selbst (Millisekunden pro Datei).
