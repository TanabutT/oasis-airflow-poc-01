"""Optional Spark ingestion for CSV source files.

Writes the same landing layout as ingest.py (landing/sales/*.parquet), so dbt does not change.
Run from the oasis-spark image via the DAG when INGEST_ENGINE=spark.
"""
import os

from pyspark.sql import SparkSession

SOURCE_DIR = os.environ.get("SOURCE_DIR", "/opt/airflow/data/source")
LANDING_DIR = os.environ.get("LANDING_DIR", "/opt/airflow/data/landing")


def main() -> None:
    spark = SparkSession.builder.appName("ingest-sales").getOrCreate()
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{SOURCE_DIR}/sales/*.csv")
    )
    df.write.mode("overwrite").parquet(f"{LANDING_DIR}/sales")
    spark.stop()


if __name__ == "__main__":
    main()
