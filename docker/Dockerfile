# Ein Image für Orchestrierung und Jobs: Airflow 3.3 führt den Pipeline-DAG aus, seine Tasks
# starten die Spark-Jobs (Local Mode, Java 17, Iceberg-JARs im Image) in einem eigenen
# virtualenv, damit die Job-Abhängigkeiten (pandas, pyarrow, pyspark) nie mit Airflows eigenen
# kollidieren.
FROM apache/airflow:3.3.2-python3.12

ARG ICEBERG_VERSION=1.12.0
ARG ICEBERG_RUNTIME_SHA256=3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82
ARG ICEBERG_AWS_SHA256=dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/iceberg
RUN curl -fsSLO "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
    && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && echo "${ICEBERG_RUNTIME_SHA256}  iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" | sha256sum -c - \
    && echo "${ICEBERG_AWS_SHA256}  iceberg-aws-bundle-${ICEBERG_VERSION}.jar" | sha256sum -c -

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=/usr/python/bin/python3.12 UV_PROJECT_ENVIRONMENT=/opt/climate/.venv

WORKDIR /opt/climate
# uv und sein Download-Cache (UV_CACHE_DIR des Basis-Images) sind nur beim Build gemountet und
# landen nicht im Image.
# Abhängigkeiten zuerst: Diese Schicht wird wiederverwendet, solange sich uv.lock nicht ändert.
COPY pyproject.toml uv.lock ./
RUN --mount=from=ghcr.io/astral-sh/uv:0.11.19,source=/uv,target=/usr/local/bin/uv \
    --mount=type=cache,target=/tmp/.cache/uv \
    uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN --mount=from=ghcr.io/astral-sh/uv:0.11.19,source=/uv,target=/usr/local/bin/uv \
    --mount=type=cache,target=/tmp/.cache/uv \
    uv sync --locked --no-dev --no-editable \
    && rm -rf src

COPY --chown=airflow:root airflow/dags /opt/airflow/dags
USER airflow
WORKDIR /opt/airflow

ENV SPARK_JARS=/opt/iceberg/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar,/opt/iceberg/iceberg-aws-bundle-${ICEBERG_VERSION}.jar
