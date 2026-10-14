"""ELT pipeline: ingest (Polars or Spark) -> dbt build on DuckDB -> archive marts to Iceberg on RustFS."""
from __future__ import annotations

import os

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task
from docker.types import Mount

HOST_DATA_DIR = os.environ["HOST_DATA_DIR"]
INGEST_ENGINE = os.environ.get("INGEST_ENGINE", "polars")
DBT_IMAGE = os.environ.get("DBT_IMAGE", "oasis-dbt:latest")
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "oasis-spark:latest")

DATA_MOUNT = Mount(source=HOST_DATA_DIR, target="/opt/airflow/data", type="bind")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["elt", "dbt"],
)
def pipeline_elt():
    if INGEST_ENGINE == "spark":
        ingest = DockerOperator(
            task_id="ingest_spark",
            image=SPARK_IMAGE,
            command="/opt/spark/bin/spark-submit --master local[*] /app/pipeline/spark_job.py",
            mounts=[DATA_MOUNT],
            auto_remove="success",
            mount_tmp_dir=False,
        )
    else:
        @task(task_id="ingest_polars")
        def ingest_polars():
            from pipeline.ingest import run

            run()

        ingest = ingest_polars()

    dbt_build = DockerOperator(
        task_id="dbt_build",
        image=DBT_IMAGE,
        command="build",
        mounts=[DATA_MOUNT],
        environment={"DBT_DUCKDB_PATH": "/opt/airflow/data/warehouse/analytics.duckdb"},
        auto_remove="success",
        mount_tmp_dir=False,
    )

    @task(task_id="archive_iceberg")
    def archive_iceberg():
        from airflow.sdk import get_current_context

        from pipeline.export_iceberg import main

        run_date = get_current_context()["dag_run"].run_after.strftime("%Y-%m-%d")
        main(run_date=run_date)

    ingest >> dbt_build >> archive_iceberg()


pipeline_elt()
