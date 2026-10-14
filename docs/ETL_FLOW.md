# ETL flow and repo guide

Team walkthrough: how data moves through this repo, what each folder does, and how work is developed, tested and validated.

## 1. The big picture

Two pipelines run on the same local Docker stack (Airflow + Postgres + RustFS + DuckDB + Iceberg).

| Pipeline (DAG) | Input | Output | Purpose |
| --- | --- | --- | --- |
| `indicators_etl` | JSON files in `data/source/indicators/` | Bronze files in RustFS, silver Iceberg tables in RustFS | Clean indicator metadata into silver |
| `pipeline_elt` | CSV files in `data/source/sales/` | DuckDB file `data/warehouse/analytics.duckdb`, mart archived to Iceberg in `oasis-archive` | Sales ELT with dbt |

```
                        ┌──────────────────────── Airflow (orchestrates) ────────────────────────┐
                        │                                                                         │
 indicators_etl         │  ingest_bronze ──► clean_silver ──► load_silver                         │
 data/source/indicators │      │                 │                 │                                │
 (JSON)                 │      ▼                 ▼                 ▼                                │
                        │  bronze bucket   silver/_staging    silver Iceberg tables               │
                        │  (raw JSON)      (Parquet)          (indicators, indicator_topics)      │
                        │                                                                         │
 pipeline_elt           │  ingest_polars ──► dbt_build ──► archive_iceberg                         │
 data/source/sales      │  (Polars→Parquet)  (dbt in its   (mart → Iceberg in oasis-archive)       │
 (CSV)                  │                     own container)                                      │
                        └─────────────────────────────────────────────────────────────────────────┘
```

## 2. Storage layers (RustFS buckets)

RustFS is an S3-compatible store, run as a container. The buckets follow the medallion pattern.

| Bucket | Layer | What is in it |
| --- | --- | --- |
| `bronze` | Raw | Source files exactly as received, plus `_manifest.json` (sha256, bytes, record counts). Path: `indicators/ingest_date=YYYY-MM-DD/batch_id=<run>/` |
| `silver` | Cleaned | Iceberg tables `silver.indicators` and `silver.indicator_topics` under `warehouse/`. Staged Parquet for the current batch under `_staging/` |
| `gold` | Business-ready | Created, not populated yet |
| `oasis-archive` | Legacy | Sales mart archive (`analytics.mart_daily_sales`) from `pipeline_elt` |

The Iceberg catalog (table metadata) is a SQL catalog stored in the Postgres database `iceberg_catalog`. Only the data files live in RustFS.

## 3. Walkthrough: `indicators_etl`, step by step

Using the sample data in `data/source/indicators/indicators_sample.json` (3 records).

**Step 1: `ingest_bronze`** (`dags/indicators_etl.py` → `pipeline/indicators.py: ingest_bronze`)
- Reads every `*.json` in the source folder.
- Checks each file parses as JSON. Stops on invalid JSON.
- Copies the bytes to `bronze/indicators/ingest_date=…/batch_id=…/` unchanged.
- Writes `_manifest.json` with sha256, byte count and record count per file.
- Returns the file keys to the next task (XCom).

**Step 2: `clean_silver`** (`pipeline/indicators.py: clean_silver` → `transform_local`)
- Downloads the batch's files from bronze into a temp folder.
- Loads them into DuckDB with `read_json_auto`, which infers the nested structure.
- Runs two SQL files from `pipeline/sql/`:
  - `silver_indicators.sql`: one row per indicator. Trims text, turns blanks into NULL, unescapes `\/`, casts the source id to integer.
  - `silver_indicator_topics.sql`: one row per indicator-topic pair. Explodes the `topics` array and drops empty `{}` placeholders. Casts `topic_id` to integer.
- Runs the quality checks (section 5). Any failure stops the run before anything is written.
- Writes the two tables to Parquet under `silver/_staging/<table>/batch_id=…/`.

**Step 3: `load_silver`** (`pipeline/indicators.py: load_silver`)
- Reads the staged Parquet.
- `indicators`: upsert on `indicator_id`. Re-running a batch updates rows, it doesn't duplicate them.
- `indicator_topics`: deletes the existing topic rows for the batch's indicator ids, then appends the new ones. A removed topic doesn't linger.
- Writes to Iceberg under `silver/warehouse/silver/<table>/` (`data/` and `metadata/` folders).

### Example: how one topic travels

Input record `3.2.TheilInd1` has `"topics": [{"id": "11", "value": "Poverty "}]`.

1. Bronze keeps the raw string `"11"` and `"Poverty "` (trailing space).
2. DuckDB explodes the array into one row, casts `"11"` to `11`, trims `"Poverty "` to `Poverty`.
3. The row is staged at `silver/_staging/indicator_topics/batch_id=…/indicator_topics.parquet`.
4. `load_silver` appends it to `silver.indicator_topics`, in the table's data folder.

`topic_id` is a column, not a folder. The table isn't partitioned by it.

## 4. Walkthrough: `pipeline_elt` (sales)

1. **`ingest_polars`** (`pipeline/ingest.py`): reads CSV files from `data/source/sales/` with Polars, writes `data/landing/sales/sales.parquet`. Optional REST API source is written to `data/landing/api_orders/` when `API_BASE_URL` is set.
2. **`dbt_build`**: the DAG starts the `oasis-dbt` container through the Docker socket. Inside it, dbt builds `stg_sales` (view) and `mart_daily_sales` (table) in `data/warehouse/analytics.duckdb`, and runs 7 tests.
3. **`archive_iceberg`** (`pipeline/export_iceberg.py`): copies `mart_daily_sales` from DuckDB into the Iceberg table `analytics.mart_daily_sales` in `oasis-archive`. Each run replaces only its own `archive_ds` slice.

The DAG runs `ingest` → `dbt_build` → `archive_iceberg` in that order, because DuckDB allows only one writer.

## 5. Validation

Validation happens at four points:

| When | What | Where |
| --- | --- | --- |
| Ingest | Invalid JSON stops the run. Each file's sha256 and record count go into the manifest. | `ingest_bronze` |
| Clean | Six quality checks, each must return 0: indicator id present, id unique in batch, name present, no bronze record lost, topic id present, every topic belongs to an indicator | `QUALITY_CHECKS` in `pipeline/indicators.py` |
| Load | Upsert on `indicator_id` makes reruns idempotent. The topic delete-and-append is replaced per batch. | `load_silver` |
| Sales | dbt tests: `not_null` and `unique` on `stg_sales.order_id`, `not_null` on mart columns | `dbt/models/schema.yml` |

## 6. Folder guide

### Repo root
| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Base stack: Postgres, RustFS, bucket init, Airflow services, and the image-only `dbt` and `spark` services (profile `tools`), plus an optional Spark cluster (profile `spark-cluster`) |
| `docker-compose.local.yml` | Local override. Binds the Airflow UI and RustFS console to `127.0.0.1` |
| `Dockerfile` | Airflow image: `apache/airflow:slim-3.3.2-python3.12` plus `requirements.txt`, installed with the official constraints file |
| `requirements.txt` | Python packages for Airflow tasks: polars, duckdb (pinned, must match `dbt/Dockerfile`), pyiceberg, pyarrow, httpx, providers |
| `Makefile` | Entry points: `local-up`, `local-down`, `trigger`, `trigger-indicators`, `dbt-build`, `iceberg-db`, `spark-cluster-up` |
| `.env.example` | Template for `.env.local`. Never commit `.env.local`, it holds secrets |
| `.dockerignore`, `.gitignore` | Keep secrets, data and build junk out of images and git |
| `README.md` | Setup and usage |

### `dags/`: Airflow DAG definitions
| File | Purpose |
| --- | --- |
| `indicators_etl.py` | The indicators pipeline: `ingest_bronze` → `clean_silver` → `load_silver`. TaskFlow tasks, each imports from `pipeline/` |
| `pipeline_elt.py` | The sales pipeline: `ingest_polars` → `dbt_build` (DockerOperator) → `archive_iceberg` |
| `hello_word.py`, `new_dag.py` | Not part of this pipeline. Created outside this work. Not reviewed here |

Airflow reads this folder at runtime. Edits take effect without a rebuild.

### `pipeline/`: Python code the tasks run
| File | Purpose |
| --- | --- |
| `lakehouse.py` | Shared helpers: RustFS filesystem (`s3_fs`), bucket names, Iceberg catalog factory (`get_catalog`) |
| `indicators.py` | All indicators logic: bronze copy and manifest, DuckDB transform, quality checks, staging, Iceberg load |
| `ingest.py` | Sales ingestion with Polars: CSV, Excel, Parquet, JSON files plus the optional REST API |
| `export_iceberg.py` | Sales archive: mart from DuckDB to Iceberg |
| `spark_job.py` | Optional Spark version of sales ingestion (CSV only). Used when `INGEST_ENGINE=spark` |
| `sql/silver_indicators.sql` | Silver cleaning for indicators (trim, null, unescape, type) |
| `sql/silver_indicator_topics.sql` | Silver cleaning and explode for topics |
| `__init__.py` | Makes `pipeline` an importable package |

### `dbt/`: SQL transformation for sales
| File | Purpose |
| --- | --- |
| `dbt_project.yml` | Project config. Staging models are views, marts are tables |
| `profiles.yml` | DuckDB connection. Path comes from `DBT_DUCKDB_PATH` |
| `models/staging/stg_sales.sql` | Reads landing Parquet, types the columns, computes `amount` |
| `models/marts/mart_daily_sales.sql` | Daily totals by date and region |
| `models/schema.yml` | Tests for both models |
| `Dockerfile` | `dbt-duckdb` image, run by `pipeline_elt` through DockerOperator |

### `data/`: inputs and outputs (git-ignored except the samples)
| Path | Purpose |
| --- | --- |
| `source/indicators/` | Input JSON for `indicators_etl`. Holds the sample file |
| `source/sales/` | Input CSV for `pipeline_elt`. Holds the sample file |
| `landing/` | Output of `ingest_polars`: Parquet for dbt. Generated, git-ignored |
| `warehouse/` | `analytics.duckdb`, the dbt output. Generated, git-ignored |

### `tests/`
| File | Purpose |
| --- | --- |
| `test_indicators_clean.py` | Six tests on the silver SQL, run on the sample JSON. Needs no S3 or Postgres |

### `spark/`
| File | Purpose |
| --- | --- |
| `Dockerfile` | Spark image (`apache/spark`) with `spark_job.py`. Used by the optional Spark ingest and the cluster profile |

### `postgres-init/`
| File | Purpose |
| --- | --- |
| `01-create-iceberg-db.sql` | Creates the `iceberg_catalog` database on first start of a new Postgres volume. Existing volumes use `make iceberg-db` |

### `scripts/`
| File | Purpose |
| --- | --- |
| `gen-secrets.sh` | Generates the Fernet key and JWT secret for `.env.local` |

### Runtime folders (created by Docker, git-ignored)
| Folder | Purpose |
| --- | --- |
| `logs/` | Airflow task logs |
| `plugins/`, `config/` | Mounted into Airflow. Empty for now |

## 7. How we develop

- **Start the stack:** `make local-up` builds the images and starts everything. `make trigger-indicators` or `make trigger` runs a pipeline.
- **Change Python or SQL:** edit files in `pipeline/`, `dags/` or `dbt/`. Airflow reads these from bind mounts, so no rebuild is needed. The next task run uses the new code.
- **Change dependencies or images:** edit `requirements.txt`, `Dockerfile` or `dbt/Dockerfile`, then run `make local-up` again to rebuild.
- **Add a silver column:** change the SQL in `pipeline/sql/`, add a test in `tests/`, run the DAG.
- **Keep the duckdb version in sync:** `requirements.txt` and `dbt/Dockerfile` both pin `duckdb`. The archive step reads files written by dbt.

## 8. How we test

1. **Unit tests (fast, no services):** `PYTHONPATH=. pytest tests/`. Covers the silver SQL on the sample. Run before every commit.
2. **Pipeline run (integration):** `make trigger-indicators`, then check the Airflow UI. All three tasks should be green.
3. **Rerun check:** trigger the same DAG twice. Row counts in silver must stay the same. Expected: `silver.indicators` 3 rows, `silver.indicator_topics` 1 row for the sample.
4. **Sales run:** `make trigger` runs `pipeline_elt`. dbt runs its 7 tests inside the dbt container.

Not automated yet: CI, and a test that runs the full DAG on every change.

## 9. How we validate data

- Quality checks in `clean_silver` must pass. A failure stops the batch.
- The bronze manifest proves what was received: file hash and record count.
- Read back from Iceberg to confirm: the table scan returns the expected rows and columns.
- Compare silver counts against bronze counts: "no bronze record lost" is one of the checks.

## 10. Open items

- **Schema is implied, not declared.** The Iceberg table takes its types from the first batch's SQL casts. An explicit silver schema in Python is the planned fix.
- **Old staging files are never deleted.** `_staging/` grows with each run.
- **Old Iceberg files stay until snapshots expire.** Each write adds data and metadata files.
- **Gold is empty.** Needs a definition of the business outputs.
- **Only one source for indicators** (the sample JSON). A REST source would be a separate addition.
- **No schedule.** Both DAGs run only when triggered.
- **Docker socket** is mounted into Airflow for the dbt container. It is root-equivalent on the host. Acceptable for local use only.
