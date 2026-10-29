# Superset with DuckDB as query engine: DuckDB reads the gold tables straight from the Iceberg
# REST catalog (ADR 0002), so the dashboard needs no copy of the data and no extra database.
FROM apache/superset:6.1.0-py312

USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python duckdb==2.0.0 duckdb-engine==0.17.0
USER superset

# The extensions are downloaded at build time, so the container needs no internet at runtime.
RUN /app/.venv/bin/python -c "import duckdb; c = duckdb.connect(); c.sql('INSTALL iceberg'); c.sql('INSTALL httpfs')"

COPY --chown=superset docker/superset/superset_config.py /app/pythonpath/superset_config.py
