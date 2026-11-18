# Superset mit DuckDB als Query-Engine: DuckDB liest die Gold-Tabellen direkt über den
# Iceberg-REST-Katalog (ADR 0002), das Dashboard braucht also keine Datenkopie und keine
# zusätzliche Datenbank.
FROM apache/superset:6.1.0-py312

USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python duckdb==2.0.0 duckdb-engine==0.17.0
USER superset

# Die Extensions werden beim Build geladen, der Container braucht zur Laufzeit kein Internet.
RUN /app/.venv/bin/python -c "import duckdb; c = duckdb.connect(); c.sql('INSTALL iceberg'); c.sql('INSTALL httpfs')"

COPY --chown=superset docker/superset/superset_config.py /app/pythonpath/superset_config.py
