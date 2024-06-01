from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

def get_daily_data():

    urls = ["https://api.data.gov.my/data-catalogue?id=covid_cases&limit=1",
            "https://api.data.gov.my/data-catalogue?id=covid_cases_vaxstatus&limit=1",
            "https://api.data.gov.my/data-catalogue?id=covid_cases_age&limit=1"]

    datasets = list()

    for url in urls:
        response_json = requests.get(url=url).json()
        datasets.append(response_json)

    print(datasets)


default_args = {
    "owner": "kelvin",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="covid19_project_dag",
    description="this is a 5011CEM Big Data Project",
    start_date = datetime(2024, 6, 1, 12),
    schedule_interval="@daily",
    default_args=default_args

) as dag:
    t1 = PythonOperator(
        task_id = "get_data",
        python_callable=get_daily_data
    )

    t1