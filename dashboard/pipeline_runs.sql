-- Dataset "pipeline_runs": letzte Läufe je Job mit Status, Dauer und geschriebenen Zeilen.
-- Chart: Pipeline-Läufe (Tabelle).
SELECT
    started_at,
    job,
    status,
    round(duration_s, 0) AS duration_s,
    rows_written,
    coalesce(error, '') AS error
FROM lakehouse.ops.pipeline_runs
WHERE job NOT IN ('benchmark', 'rollback')
ORDER BY started_at DESC
LIMIT 50
