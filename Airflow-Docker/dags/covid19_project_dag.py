from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts import tasks


default_args = {
    "owner": "kelvin",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="covid19_project_dag_epidemic",
    description="this is a 5011CEM Big Data Project, this dag manages epidemic data",
    start_date = datetime(2024, 6, 2, 0),
    schedule_interval="@weekly",
    default_args=default_args

) as dag:
    t1 = PythonOperator(
        task_id = "get_data",
        python_callable=tasks.get_weekly_epidemic_data
    )
    t2 = PythonOperator(
        task_id="data_preprocessing",
        python_callable=tasks.consolidate_epidemic_data
    )


    t1>>t2