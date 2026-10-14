FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    DBT_PROFILES_DIR=/dbt \
    DBT_TARGET_PATH=/tmp/dbt_target \
    DBT_LOG_PATH=/tmp/dbt_logs

# duckdb is pinned to the same version the Airflow image uses (archive step reads the .duckdb file)
RUN pip install --no-cache-dir dbt-duckdb==1.11.0 duckdb==1.5.6

WORKDIR /dbt
COPY . /dbt

ENTRYPOINT ["dbt"]
CMD ["build"]
