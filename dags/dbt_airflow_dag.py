from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 4, 1)
}

with DAG(
    dag_id='ny_taxi_data_model',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /var/tmp && dbt run'
    )

    dbt_run 
