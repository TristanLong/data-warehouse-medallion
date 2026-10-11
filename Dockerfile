# syntax=docker/dockerfile:1

FROM python:3.11-slim-bookworm

ARG MSSQL_JDBC_VERSION=13.4.0.jre11

LABEL org.opencontainers.image.title="data-warehouse-medallion-etl"
LABEL org.opencontainers.image.description="PySpark medallion data warehouse ETL pipeline"

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DB_JDBC_JAR=/opt/jdbc/mssql-jdbc.jar

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl openjdk-17-jre-headless \
    && mkdir -p /opt/jdbc \
    && curl --fail --location --silent --show-error \
        "https://maven-central.storage-download.googleapis.com/maven2/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar" \
        --output /opt/jdbc/mssql-jdbc.jar \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

COPY --chown=10001:10001 main.py /app/main.py
COPY --chown=10001:10001 src /app/src
COPY --chown=10001:10001 data /app/data

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin etl \
    && mkdir -p /app/.cache/ivy \
    && chown -R etl:etl /app

USER etl

CMD ["python", "main.py"]
