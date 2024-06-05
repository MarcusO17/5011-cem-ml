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
    dag_id="covid19_project_dag",
    description="this is a 5011CEM Big Data Project",
    start_date = datetime(2024, 6, 2, 0),
    schedule_interval="@weekly",
    default_args=default_args

) as dag:
    t1 = PythonOperator(
        task_id = "get_epidemic_data",
        python_callable=tasks.get_weekly_epidemic_data
    )
    t2 = PythonOperator(
        task_id="epidemic_data_preprocessing",
        python_callable=tasks.consolidate_epidemic_data
    )
    t3 = PythonOperator(
        task_id="get_vaccination_data",
        python_callable=tasks.get_weekly_vaccination_data
    )

    t4 = PythonOperator(
        task_id="vaccination_data_preprocessing",
        python_callable=tasks.consolidate_vaccination_data
    )
    t5 = PythonOperator(
        task_id="epidemic_data_cleaning",
        python_callable=tasks.data_cleaning
    )
    t6 = PythonOperator(
        task_id="epidemic_data_cleaning_2",
        python_callable=tasks.data_cleaning_2
    )


    t1>>t2>>t5>>t6

    t3>>t4