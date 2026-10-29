# ADR 0004: Gold-Kennzahlen, Referenzperiode und Überschreiben je Dekade

- Status: angenommen
- Datum: 2026-10-29

## Kontext
Die Leitfrage vergleicht Bundesländer und Jahrzehnte mit der Referenzperiode 1961–1990. Zwei
Dinge verzerren einen naiven Vergleich:
1. **Lücken:** Fehlen an einer Station drei Sommermonate, sieht ein Jahr mit Hitzetagen kühl aus.
2. **Wechselndes Stationsnetz:** Kommt in den 2010ern eine Station an einem wärmeren Ort hinzu,
   steigt der Mittelwert eines Landes, ohne dass es dort wärmer geworden ist.

## Entscheidung
- **`gold.station_year`**: eine Zeile je Station und Jahr mit Hitzetagen (Tmax ≥ 30 °C),
  Frosttagen (Tmin < 0 °C), Starkregentagen (≥ 20 mm), Sonnenstunden und Mitteltemperatur.
  Eine Kennzahl ist `NULL`, wenn der benötigte Wert an weniger als 330 Tagen (~90 %) vorliegt.
- **`gold.state_decade`**: Mittel je Bundesland und Dekade. Die Abweichung zu 1961–1990
  (`*_vs_1961_1990`) ist das Mittel der **Anomalien je Station** (Wert minus eigenes
  Referenzmittel). Stationen ohne Referenzjahre zählen im Mittelwert, nicht in der Abweichung.
- **Schreiben:** `station_year` ist nach `truncate(10, year)` partitioniert und wird per
  `overwritePartitions()` (Iceberg: dynamisches `INSERT OVERWRITE`) nur für Dekaden neu
  geschrieben, in denen Silver seit dem letzten Lauf Zeilen geändert hat (Grenze
  `climate.silver-loaded-until`). `state_decade` hat wenige hundert Zeilen und hängt von allen
  Referenzjahren ab; es wird bei jeder Änderung vollständig ersetzt.
- **Gates vor dem Schreiben:** eindeutiger Schlüssel (Station, Jahr) und keine unmöglichen Werte
  (mehr Hitzetage als beobachtete Tage, mehr als 17 Sonnenstunden je Tag).

## Verworfene Alternativen
- **Partition je Jahr:** bei 50 Stationen 76 Partitionen mit je einer Datei von wenigen KB.
  Je Dekade sind es 8 Partitionen, und ein Update betrifft fast immer nur die laufende.
- **MERGE wie in Silver:** Gold wird aus Silver vollständig ableitbar neu berechnet; ein
  Überschreiben ganzer Partitionen ist einfacher und genauso idempotent.
- **Landesmittel direkt mit dem Landesmittel 1961–1990 vergleichen:** einfacher, aber genau die
  Verzerrung durch das wechselnde Stationsnetz aus Punkt 2.

## Konsequenzen
- Das laufende Jahr hat noch keine Kennzahlen (zu wenige Tage); die Dekade 2020 umfasst
  2020 bis zum letzten vollständigen Jahr.
- Verschwindet eine ganze Dekade aus Silver (z. B. nach Erhöhung von `START_YEAR`), bleibt sie
  in Gold bis `make gold-full`.
- `reconcile` prüft nach jedem Lauf: alle Raw-Dateiversionen in Bronze, alle Bronze-Tage
  (ab `START_YEAR`) in Silver gültig oder abgelehnt, alle gültigen Silver-Tage in Gold gezählt.
