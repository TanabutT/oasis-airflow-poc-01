# oasis-airflow-poc-01

ELT pipeline on local Docker: **Airflow** orchestrates, **Polars** (or optional **Spark**) ingests,
**dbt + DuckDB** transform in SQL, and marts are archived as **Iceberg** tables on **RustFS** (S3 API).

```
files (CSV/XLSX/Parquet) ─┐
REST API (JSON) ──────────┴─► ingest (Polars | Spark) ─► data/landing/*.parquet
                                                              │
                                  dbt build (DuckDB) ─► data/warehouse/analytics.duckdb
                                                              │
                                  archive_iceberg ─► RustFS bucket (Iceberg, catalog in Postgres)
```

## Quick start (local)
```bash
cp .env.example .env.local
./scripts/gen-secrets.sh >> .env.local      # then delete the empty AIRFLOW_FERNET_KEY= / AIRFLOW_JWT_SECRET= lines
# edit passwords and RustFS keys in .env.local
make local-up                                # builds the images, starts the stack
```
- Airflow UI: http://localhost:8080 (login: `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`)
- RustFS console: http://localhost:9001 (login: `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY`)
- Trigger the pipeline: `make trigger` (or the Airflow UI, DAG `pipeline_elt`)
- Run dbt by hand: `make dbt-build`

Stop: `make local-down`. Wipe all data including the database: `make local-reset`.

## Repo layout
| Path | What it is |
| --- | --- |
| `dags/pipeline_elt.py` | The DAG: ingest → dbt build → archive |
| `pipeline/ingest.py` | Polars ingestion (files + optional REST API) |
| `pipeline/spark_job.py` | Optional Spark ingestion (CSV) |
| `pipeline/export_iceberg.py` | Marts from DuckDB → Iceberg on RustFS |
| `dbt/` | dbt project (`models/staging`, `models/marts`, `schema.yml` tests) and its image |
| `spark/Dockerfile` | Image for the Spark job and the optional cluster |
| `data/source/` | Source files (sample CSV included) |
| `data/landing/`, `data/warehouse/` | Generated Parquet and the DuckDB file (git-ignored) |

## Adding a file source
1. Put files in `data/source/<dataset>/`.
2. Add `<dataset>` to `FILE_DATASETS` in `pipeline/ingest.py`.
3. Add `dbt/models/staging/stg_<dataset>.sql` reading `{{ var("landing_dir") }}/<dataset>/*.parquet`.
4. Add marts and tests, then `make local-up` (rebuilds only if images changed) and trigger.

## REST API source
Set `API_BASE_URL`, `API_TOKEN` (if needed), and `API_ORDERS_PATH` in `.env.local`. Ingestion writes
`data/landing/api_orders/`. The paging logic in `fetch_api` is a placeholder until the real API is confirmed,
and there is no staging model for `api_orders` yet.

## Spark option
- Set `INGEST_ENGINE=spark` in `.env.local`. The DAG then runs `spark_job.py` in the `oasis-spark` image (local mode) instead of Polars.
- Standalone cluster for experiments: `make spark-cluster-up`, then Spark UI at http://localhost:8081.
  The cluster is not used by the DAG yet.
- Spark reads CSV only. Excel and JSON stay on the Polars path.

## Upgrading an existing Postgres volume
The init script runs only when the Postgres volume is created. For an existing volume, run once:
`make iceberg-db`

## Security notes
- The `airflow-*` containers mount `/var/run/docker.sock`. That is root-equivalent on the host. Acceptable for this
  local POC, but remove it if the DockerOperator path is not needed.
- Secrets live only in `.env.local` (git-ignored). Keep the Fernet and JWT keys stable across restarts.
- RustFS is pinned to `1.0.1` (`RUSTFS_IMAGE_TAG`). Bump it deliberately, and test the archive step after any change.

## Medallion layers (RustFS)
| Bucket | Holds |
| --- | --- |
| `bronze` | Raw source files exactly as received, plus `_manifest.json` (sha256, bytes, record counts). Path: `indicators/ingest_date=YYYY-MM-DD/batch_id=<run>/` |
| `silver` | Iceberg tables `silver.indicators` and `silver.indicator_topics` (catalog in Postgres), plus `_staging/` Parquet for the current batch |
| `gold` | Created, not populated yet |
| `oasis-archive` | Legacy location of the `pipeline_elt` sales mart archive |

## Indicators ETL (`dags/indicators_etl.py`)
Source JSON in `data/source/indicators/` → `ingest_bronze` → `clean_silver` → `load_silver`.
- **Bronze:** copies the JSON unchanged.
- **Silver cleaning:** DuckDB SQL in `pipeline/sql/` trims text, turns blanks into NULL, unescapes `\/`, types the ids, explodes `topics`, and drops empty placeholders. Quality checks run before anything is written. Failing checks stop the run.
- **Silver load:** `indicators` is upserted on `indicator_id`. A batch's topics replace the old ones for those indicators. Re-running a batch doesn't duplicate rows.

Run it: `make trigger-indicators`. Tests for the cleaning SQL: `PYTHONPATH=. pytest tests/` (no S3 or Postgres needed).
