# HOST_DATA_DIR must be an absolute host path: the DAG bind-mounts it into dbt/Spark containers.
export HOST_DATA_DIR := $(CURDIR)/data
C = docker compose -f docker-compose.yml -f docker-compose.local.yml --env-file .env.local

.PHONY: trigger-indicators images local-up local-down local-logs local-reset trigger dbt-build iceberg-db spark-cluster-up

images:
	$(C) --profile tools build

local-up: images
	$(C) up -d

local-down:
	$(C) down

local-logs:
	$(C) logs -f --tail=100

local-reset:
	$(C) down -v

trigger:
	$(C) exec airflow-scheduler airflow dags trigger pipeline_elt

dbt-build:
	$(C) --profile tools run --rm dbt build

# For an existing Postgres volume (the init script only runs on a fresh volume)
iceberg-db:
	$(C) exec postgres psql -U $${POSTGRES_USER:-airflow} -c "CREATE DATABASE iceberg_catalog;"

spark-cluster-up:
	$(C) --profile spark-cluster up -d spark-master spark-worker

trigger-indicators:
	$(C) exec airflow-scheduler airflow dags trigger indicators_etl
