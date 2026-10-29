-- Dataset "germany_year": Deutschland je Jahr, Mittel über alle Stationen.
-- Abweichungen je Station gegen ihr eigenes Mittel 1961-1990, dann gemittelt (ADR 0004).
-- Charts: Temperaturabweichung, Hitze- und Frosttage, Sonnenstunden.
WITH reference AS (
    SELECT
        station_id,
        avg(temp_mean_c) AS temp_ref,
        avg(sunshine_h) AS sunshine_ref
    FROM lakehouse.gold.station_year
    WHERE year BETWEEN 1961 AND 1990
    GROUP BY station_id
)
SELECT
    make_date(y.year, 1, 1) AS year,
    count(y.temp_mean_c) AS stations,
    round(avg(y.temp_mean_c - r.temp_ref), 2) AS temp_mean_vs_1961_1990_c,
    round(avg(y.heat_days), 1) AS heat_days,
    round(avg(y.frost_days), 1) AS frost_days,
    round(avg(y.tropical_nights), 2) AS tropical_nights,
    round(avg(y.sunshine_h - r.sunshine_ref), 0) AS sunshine_vs_1961_1990_h
FROM lakehouse.gold.station_year AS y
JOIN reference AS r USING (station_id)
GROUP BY y.year
ORDER BY y.year
