# Optional Spark image: used by the DAG when INGEST_ENGINE=spark, and by the spark-cluster profile.
# Build context is the repo root so the pipeline code can be copied in.
ARG SPARK_IMAGE_TAG=4.0.0-python3
FROM apache/spark:${SPARK_IMAGE_TAG}

COPY pipeline/spark_job.py /app/pipeline/spark_job.py
