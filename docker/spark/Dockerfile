# Spark jobs (bronze, silver) in local mode: Python 3.12, Java 21, PySpark from uv.lock and the
# Iceberg jars baked in, so a job run needs no Maven access.
FROM python:3.12.16-slim-trixie

ARG ICEBERG_VERSION=1.12.0
ARG ICEBERG_RUNTIME_SHA256=3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82
ARG ICEBERG_AWS_SHA256=dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/iceberg
RUN curl -fsSLO "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
    && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && echo "${ICEBERG_RUNTIME_SHA256}  iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" | sha256sum -c - \
    && echo "${ICEBERG_AWS_SHA256}  iceberg-aws-bundle-${ICEBERG_VERSION}.jar" | sha256sum -c -

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never

WORKDIR /app
# uv and its download cache are only mounted while building, so neither ends up in the image.
# Dependencies first: this layer is reused as long as uv.lock does not change.
COPY pyproject.toml uv.lock ./
RUN --mount=from=ghcr.io/astral-sh/uv:0.11.19,source=/uv,target=/usr/local/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN --mount=from=ghcr.io/astral-sh/uv:0.11.19,source=/uv,target=/usr/local/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

RUN useradd --create-home --uid 1000 spark
USER spark

ENV PATH=/app/.venv/bin:$PATH \
    SPARK_JARS=/opt/iceberg/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar,/opt/iceberg/iceberg-aws-bundle-${ICEBERG_VERSION}.jar
