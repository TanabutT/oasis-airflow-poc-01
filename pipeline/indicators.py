"""Indicators ETL: JSON -> bronze (raw, as received) -> silver (cleaned, Iceberg on RustFS).

Steps (each is a DAG task, see dags/indicators_etl.py):
  ingest_bronze  copy the source JSON files to bronze/ unchanged, with a manifest (hashes, record counts)
  clean_silver   DuckDB SQL cleans and types the records, quality checks run, staged Parquet is written
  load_silver    upserts silver.indicators and replaces silver.indicator_topics for this batch in Iceberg
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.expressions import In

from pipeline.lakehouse import BRONZE_BUCKET, SILVER_BUCKET, get_catalog, s3_fs

log = logging.getLogger(__name__)

SOURCE_DIR = Path(os.environ.get("INDICATORS_SOURCE_DIR", "/opt/airflow/data/source/indicators"))
SQL_DIR = Path(__file__).parent / "sql"
NAMESPACE = "silver"

# Each check is a query that must return 0. A non-zero result fails the task before anything reaches Iceberg.
QUALITY_CHECKS = {
    "indicator_id is present": "select count(*) from silver_indicators where indicator_id is null",
    "indicator_id is unique in batch": "select count(*) - count(distinct indicator_id) from silver_indicators",
    "indicator_name is present": "select count(*) from silver_indicators where indicator_name is null",
    "no bronze record lost": "select (select count(*) from bronze_raw) - (select count(*) from silver_indicators)",
    "topic_id is present": "select count(*) from silver_indicator_topics where topic_id is null",
    "topic belongs to an indicator": (
        "select count(*) from silver_indicator_topics t "
        "left join silver_indicators i using (indicator_id) where i.indicator_id is null"
    ),
}


def transform_local(files: list[Path], batch_id: str) -> dict[str, pa.Table]:
    """Clean the bronze JSON files with DuckDB SQL and return the silver tables. Pure local function, no S3."""
    con = duckdb.connect()
    try:
        con.execute("create table bronze_raw as select * from read_json_auto(?, union_by_name=true) limit 0", [str(files[0])])
        for f in files:
            con.execute("insert into bronze_raw by name select * from read_json_auto(?, union_by_name=true)", [str(f)])

        params = {"batch_id": batch_id}
        con.execute(f"create table silver_indicators as {(SQL_DIR / 'silver_indicators.sql').read_text()}", params)
        con.execute(f"create table silver_indicator_topics as {(SQL_DIR / 'silver_indicator_topics.sql').read_text()}", params)

        failures = []
        for name, query in QUALITY_CHECKS.items():
            bad = con.execute(query).fetchone()[0]
            if bad:
                failures.append(f"{name}: {bad}")
        if failures:
            raise ValueError("silver quality checks failed: " + "; ".join(failures))

        return {
            "indicators": con.sql("select * from silver_indicators").to_arrow_table(),
            "indicator_topics": con.sql("select * from silver_indicator_topics").to_arrow_table(),
        }
    finally:
        con.close()


def ingest_bronze(ingest_date: str, batch_id: str) -> dict:
    """Copy source JSON to bronze unchanged, and write a manifest next to it."""
    fs = s3_fs()
    prefix = f"{BRONZE_BUCKET}/indicators/ingest_date={ingest_date}/batch_id={batch_id}"
    files = sorted(SOURCE_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"no JSON files in {SOURCE_DIR}")

    manifest = {"batch_id": batch_id, "ingest_date": ingest_date, "files": []}
    for f in files:
        raw = f.read_bytes()
        records = json.loads(raw)  # fail early on invalid JSON
        key = f"{prefix}/{f.name}"
        with fs.open(key, "wb") as out:
            out.write(raw)
        manifest["files"].append(
            {"key": key, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "records": len(records)}
        )

    manifest_key = f"{prefix}/_manifest.json"
    with fs.open(manifest_key, "wb") as out:
        out.write(json.dumps(manifest, indent=2).encode())
    log.info("bronze: %s files to %s", len(files), prefix)
    return {"batch_id": batch_id, "manifest_key": manifest_key, "file_keys": [x["key"] for x in manifest["files"]]}


def clean_silver(bronze: dict) -> dict:
    """Download the bronze batch, clean it with SQL, and stage the silver Parquet in RustFS."""
    fs = s3_fs()
    batch_id = bronze["batch_id"]
    work = Path(tempfile.mkdtemp(prefix="indicators_"))
    local_files = []
    for key in bronze["file_keys"]:
        local = work / Path(key).name
        fs.get(key, str(local))
        local_files.append(local)

    tables = transform_local(local_files, batch_id)

    staged = {"batch_id": batch_id}
    for name, table in tables.items():
        local_pq = work / f"{name}.parquet"
        pq.write_table(table, local_pq)
        key = f"{SILVER_BUCKET}/_staging/{name}/batch_id={batch_id}/{name}.parquet"
        fs.put(str(local_pq), key)
        staged[name] = key
        log.info("silver: staged %s rows of %s", table.num_rows, name)
    return staged


def _ensure_table(catalog, ident: str, schema: pa.Schema):
    try:
        return catalog.create_table(ident, schema=schema)
    except TableAlreadyExistsError:
        return catalog.load_table(ident)


def load_silver(staged: dict) -> dict:
    """Write the staged silver tables to Iceberg. Re-running the same batch leaves the same result."""
    fs = s3_fs()

    def read(key: str) -> pa.Table:
        with fs.open(key, "rb") as f:
            return pq.read_table(f)

    indicators = read(staged["indicators"])
    topics = read(staged["indicator_topics"])

    catalog = get_catalog("silver", bucket=SILVER_BUCKET)
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    ind_table = _ensure_table(catalog, f"{NAMESPACE}.indicators", indicators.schema)
    ind_table.upsert(indicators, join_cols=["indicator_id"])

    topic_table = _ensure_table(catalog, f"{NAMESPACE}.indicator_topics", topics.schema)
    ids = indicators.column("indicator_id").to_pylist()
    if ids:
        # Replace this batch's topics for these indicators, so a removed topic does not linger.
        topic_table.delete(delete_filter=In("indicator_id", ids))
    if topics.num_rows:
        topic_table.append(topics)

    counts = {"silver.indicators": indicators.num_rows, "silver.indicator_topics": topics.num_rows}
    log.info("silver load: %s", counts)
    return counts
