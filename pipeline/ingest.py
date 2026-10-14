"""Ingest raw sources into landing Parquet with Polars.

Landing layout: landing/<dataset>/<dataset>.parquet. dbt reads landing/<dataset>/*.parquet.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import httpx
import polars as pl

log = logging.getLogger(__name__)

SOURCE_DIR = Path(os.environ.get("SOURCE_DIR", "/opt/airflow/data/source"))
LANDING_DIR = Path(os.environ.get("LANDING_DIR", "/opt/airflow/data/landing"))

# Each entry maps to a folder under SOURCE_DIR holding that dataset's files.
FILE_DATASETS = ["sales"]


def read_any(path: Path) -> pl.DataFrame:
    match path.suffix.lower():
        case ".csv":
            return pl.read_csv(path)
        case ".parquet":
            return pl.read_parquet(path)
        case ".xlsx" | ".xls":
            return pl.read_excel(path)
        case ".json":
            return pl.read_json(path)
        case other:
            raise ValueError(f"unsupported file type {other!r}: {path}")


def write_landing(dataset: str, frame: pl.DataFrame) -> None:
    out_dir = LANDING_DIR / dataset
    out_dir.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(out_dir / f"{dataset}.parquet")
    log.info("wrote %s rows to %s", frame.height, out_dir)


def ingest_files(dataset: str) -> None:
    folder = SOURCE_DIR / dataset
    files = sorted(f for f in folder.glob("*") if f.is_file())
    if not files:
        raise FileNotFoundError(f"no source files for {dataset!r} in {folder}")
    frame = pl.concat([read_any(f) for f in files], how="diagonal_relaxed")
    write_landing(dataset, frame)


def fetch_api(path: str, page_size: int = 100) -> pl.DataFrame:
    """Page through a JSON list endpoint. Paging style is a placeholder until the API is confirmed."""
    base_url = os.environ["API_BASE_URL"]
    token = os.environ.get("API_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    records: list[dict] = []
    page = 1
    with httpx.Client(base_url=base_url, headers=headers, timeout=30.0) as client:
        while True:
            resp = client.get(path, params={"page": page, "per_page": page_size})
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            records.extend(batch)
            page += 1
    return pl.DataFrame(records, infer_schema_length=None)


def run() -> None:
    for dataset in FILE_DATASETS:
        ingest_files(dataset)
    if os.environ.get("API_BASE_URL"):
        write_landing("api_orders", fetch_api(os.environ.get("API_ORDERS_PATH", "/orders")))
    else:
        log.info("API_BASE_URL not set, skipping API source")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
