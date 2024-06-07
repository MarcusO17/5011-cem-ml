from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts import tasks

from supabase import create_client

supabase_url = "https://yorsqfjyqjmdzoenjynx.supabase.co"
supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlvcnNxZmp5cWptZHpvZW5qeW54Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MTc3NDk5NTksImV4cCI6MjAzMzMyNTk1OX0.m62d7CVHI49gdBPQgSRwTBy_gJlkfplxcc1wz3F6GoU"

client = create_client(supabase_url, supabase_key)

default_args = {
    "owner": "kelvin",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

query1 = '''
UPDATE "MalaysiaEpidemic"
SET
  rtk_ag = NULL,
  pcr = NULL
WHERE
  rtk_ag = -1
  OR pcr = -1;
      
'''

query2 = '''
UPDATE "StateEpidemic"
SET
  rtk_ag = NULL,
  pcr = NULL
WHERE
  rtk_ag = -1
  OR pcr = -1;
'''

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
        task_id="update_missing_testing_data",
        python_callable=tasks.update_missing_testing_data,
        op_args=[client]
    )
    t7 = PythonOperator(
        task_id="load_data",
        python_callable=tasks.load_data,
        op_args=[client],
        provide_context = True
    )
    t8 = SQLExecuteQueryOperator(
        task_id="update_placeholder_into_null_epidemic",
        conn_id="postgres_default",
        sql=query2
    )
    t9 = SQLExecuteQueryOperator(
        task_id="update_placeholder_into_null_malaysia",
        conn_id="postgres_default",
        sql=query1
    )

    t6>>t1>>t2>>t5>>t7
    t3>>t4>>t7
    t7>>t8>>t9