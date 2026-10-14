"""Indicators ETL: JSON -> bronze (raw) -> silver (cleaned, written to Iceberg on RustFS)."""
from __future__ import annotations

import re

import pendulum
from airflow.sdk import dag, get_current_context, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["elt", "bronze", "silver"],
)
def indicators_etl():
    @task
    def ingest_bronze():
        from pipeline.indicators import ingest_bronze as run

        ctx = get_current_context()
        batch_id = re.sub(r"[^A-Za-z0-9_.-]", "_", ctx["run_id"])
        ingest_date = ctx["dag_run"].run_after.strftime("%Y-%m-%d")
        return run(ingest_date=ingest_date, batch_id=batch_id)

    @task
    def clean_silver(bronze: dict):
        from pipeline.indicators import clean_silver as run

        return run(bronze)

    @task
    def load_silver(staged: dict):
        from pipeline.indicators import load_silver as run

        return run(staged)

    load_silver(clean_silver(ingest_bronze()))


indicators_etl()
