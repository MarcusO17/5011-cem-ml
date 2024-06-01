from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd

default_args = {
    "owner": "kelvin",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}
# getting latest data for current day
def get_daily_data():

    urls = ["https://api.data.gov.my/data-catalogue?id=covid_cases&limit=1",
            "https://api.data.gov.my/data-catalogue?id=covid_cases_vaxstatus&limit=1",
            "https://api.data.gov.my/data-catalogue?id=covid_cases_age&limit=1"]

    datasets = list()

    for url in urls:
        response_json = requests.get(url=url).json()
        datasets.append(response_json)

    print(datasets)
    return datasets

#combine the datasets into one row
def consolidate_data(ti):

    datasets = ti.xcom_pull(task_ids="get_data")
    combined_datasets = {}
    for dataset in datasets:
        for key,value in dataset[0].items():
            combined_datasets[key] = value

    df = pd.DataFrame([combined_datasets])
    df = df.drop_duplicates()
    pd.set_option('display.max_columns', None)

    print(df)

    return df

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
    t2 = PythonOperator(
        task_id="consolidate_data",
        python_callable = consolidate_data
    )

    t1>>t2