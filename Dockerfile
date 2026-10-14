ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Re-declare after FROM so they are usable in this stage
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
      -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
