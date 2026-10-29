-- Dataset "state_decade": Bundesland je Dekade, Abweichung zu 1961-1990 (gold.state_decade).
-- Charts: Hitzetage je Bundesland und Dekade (Heatmap), Starkregentage je Dekade.
SELECT
    state,
    cast(decade AS VARCHAR) || 'er' AS decade,
    stations,
    round(heat_days_vs_1961_1990, 1) AS heat_days_vs_1961_1990,
    round(frost_days_vs_1961_1990, 1) AS frost_days_vs_1961_1990,
    round(heavy_rain_days_vs_1961_1990, 2) AS heavy_rain_days_vs_1961_1990,
    round(temp_mean_c_vs_1961_1990, 2) AS temp_mean_c_vs_1961_1990
FROM lakehouse.gold.state_decade
WHERE decade >= 1960
ORDER BY state, decade
