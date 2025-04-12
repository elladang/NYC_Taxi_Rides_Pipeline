

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../modules')))
# dags/ny_taxi_multi_branch_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from modules.configs.settings import GCS_BUCKET, dataset_name, project_id
from modules.extract import load_data_from_api
from modules.transform import transform_yellow_data, transform_green_data, transform_zone_lookup_data
from modules.load import upload_to_gcs
from modules.load_to_bigquery import load_to_bigquery
from airflow.operators.empty import EmptyOperator

default_args = {
    'start_date': datetime(2025, 4, 1)
}

with DAG(
    dag_id='ny_taxi_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True
) as dag:

    start = EmptyOperator(task_id='start')

    # TaskGroup: Extract
    with TaskGroup('extract_group', tooltip='Extract data from API') as extract_group:
        yellow = PythonOperator(
            task_id='load_yellow',
            python_callable=load_data_from_api,
            op_kwargs={'taxi_type': 'yellow'},
        )
        green = PythonOperator(
            task_id='load_green',
            python_callable=load_data_from_api,
            op_kwargs={'taxi_type': 'green'},
        )
        zone = PythonOperator(
            task_id='load_zone',
            python_callable=load_data_from_api,
            op_kwargs={'taxi_type': 'zone'},
        )

    # TaskGroup: Transform
    with TaskGroup('transform_group', tooltip='Transform raw data') as transform_group:
        yellow = PythonOperator(
            task_id='transform_yellow',
            python_callable=transform_yellow_data,
            provide_context=True
        )
        green = PythonOperator(
            task_id='transform_green',
            python_callable=transform_green_data,
            provide_context=True
        )
        zone = PythonOperator(
            task_id='transform_zone',
            python_callable=transform_zone_lookup_data,
            provide_context=True
        )

    # TaskGroup: Upload to GCS
    with TaskGroup('upload_group', tooltip='Upload data to GCS') as upload_group:
        yellow = PythonOperator(
            task_id='upload_yellow',
            python_callable=upload_to_gcs,
            op_kwargs={'taxi_type': 'yellow'},
            provide_context=True
        )
        green = PythonOperator(
            task_id='upload_green',
            python_callable=upload_to_gcs,
            op_kwargs={'taxi_type': 'green'},
            provide_context=True
        )
        zone = PythonOperator(
            task_id='upload_zone',
            python_callable=upload_to_gcs,
            op_kwargs={'taxi_type': 'zone'},
            provide_context=True
        )

    # TaskGroup: Load to BigQuery
    with TaskGroup('bq_group', tooltip='Load to BigQuery') as bq_group:
        yellow = PythonOperator(
            task_id='bq_yellow',
            python_callable=load_to_bigquery,
            op_kwargs={'taxi_type': 'yellow'},
            provide_context=True
        )
        green = PythonOperator(
            task_id='bq_green',
            python_callable=load_to_bigquery,
            op_kwargs={'taxi_type': 'green'},
            provide_context=True
        )
        zone = PythonOperator(
            task_id='bq_zone',
            python_callable=load_to_bigquery,
            op_kwargs={'taxi_type': 'zone'},
            provide_context=True
        )

    end = EmptyOperator(task_id='end')

    # Task chaining
    start >> extract_group >> transform_group >> upload_group >> bq_group >> end