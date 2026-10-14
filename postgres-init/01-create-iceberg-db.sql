-- Runs only when the Postgres data volume is first created.
-- For an existing volume, run: make iceberg-db
CREATE DATABASE iceberg_catalog;
