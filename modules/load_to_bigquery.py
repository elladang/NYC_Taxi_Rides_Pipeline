from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from configs.settings import GCS_BUCKET, dataset_name, project_id, GCP_CONN_ID

def load_to_bigquery(taxi_type: str, **kwargs):
    execution_date = kwargs['ds'] 

    object_path = f"gs://{GCS_BUCKET}/{dataset_name}/{taxi_type}/data_date={execution_date}/{taxi_type}_transformed.parquet"
    table_id = f"{project_id}.{dataset_name}.{taxi_type}_tripdata"

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)

    bq_hook.run_load(
        destination_project_dataset_table=table_id,
        source_uris=[object_path],
        source_format='PARQUET',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
    )
