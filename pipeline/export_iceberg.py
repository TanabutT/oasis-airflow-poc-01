"""Archive dbt marts from DuckDB to Iceberg tables on RustFS (S3 API).

Catalog is a SQL catalog stored in Postgres. Each run replaces only its own archive_ds slice,
so re-running a date does not duplicate rows.
"""
from __future__ import annotations

import logging
import os
import sys

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.expressions import EqualTo

from pipeline.lakehouse import get_catalog

log = logging.getLogger(__name__)

DUCKDB_PATH = os.environ.get("DBT_DUCKDB_PATH", "/opt/airflow/data/warehouse/analytics.duckdb")
MARTS = ["mart_daily_sales"]


def main(run_date: str) -> None:
    namespace = os.environ.get("ICEBERG_NAMESPACE", "analytics")
    catalog = get_catalog("archive", bucket=os.environ["RUSTFS_BUCKET"])
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        for mart in MARTS:
            data = con.execute(f"select * from {mart}").to_arrow_table()
            data = data.append_column("archive_ds", pa.array([run_date] * data.num_rows, pa.string()))

            ident = f"{namespace}.{mart}"
            try:
                table = catalog.create_table(ident, schema=data.schema)
            except TableAlreadyExistsError:
                table = catalog.load_table(ident)
            table.overwrite(data, overwrite_filter=EqualTo("archive_ds", run_date))
            log.info("archived %s rows of %s for %s", data.num_rows, mart, run_date)
    finally:
        con.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main(sys.argv[1])
