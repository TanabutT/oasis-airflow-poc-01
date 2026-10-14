"""Shared RustFS (S3 API) and Iceberg helpers for the bronze / silver / gold layers."""
from __future__ import annotations

import os

import s3fs
from pyiceberg.catalog.sql import SqlCatalog

BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "silver")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "gold")


def s3_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=os.environ["RUSTFS_ACCESS_KEY"],
        secret=os.environ["RUSTFS_SECRET_KEY"],
        client_kwargs={"endpoint_url": os.environ["RUSTFS_ENDPOINT"]},
    )


def get_catalog(name: str, bucket: str) -> SqlCatalog:
    """Iceberg SQL catalog (metadata in Postgres), with table data stored in the given RustFS bucket."""
    return SqlCatalog(
        name,
        uri=os.environ["ICEBERG_CATALOG_URI"],
        warehouse=f"s3://{bucket}/warehouse",
        **{
            "s3.endpoint": os.environ["RUSTFS_ENDPOINT"],
            "s3.access-key-id": os.environ["RUSTFS_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["RUSTFS_SECRET_KEY"],
            "s3.path-style-access": "true",
            "s3.region": os.environ.get("RUSTFS_REGION", "us-east-1"),
        },
    )
