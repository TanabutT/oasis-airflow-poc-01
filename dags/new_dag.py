import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from hello_word import hello_real  # Ensure hello_real is a Python callable function

with DAG(
    dag_id="NEW_DAG",
    start_date=datetime(2026, 8, 6),
    schedule=None,
    # Daily scheduling is owned by main_data_pipeline (18:00 UTC / 01:00 Bangkok).
    max_active_runs=3,
    catchup=False,

) as dag:
    start = EmptyOperator(task_id="start")
    wave1_complete = EmptyOperator(task_id="wave1_complete")
    hello_real_task = PythonOperator(
            task_id="hello_real",
            python_callable=hello_real
        )
    end = EmptyOperator(task_id="end")

    # start >> wave1_complete >> hello_real_task >> end
    start
    [wave1_complete, hello_real_task] >> end
