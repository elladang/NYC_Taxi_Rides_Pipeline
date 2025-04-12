
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from configs.settings import GCS_BUCKET, dataset_name

def upload_to_gcs(taxi_type, **kwargs):
    ti = kwargs['ti']
    transform_file = ti.xcom_pull(task_ids=f'transform_group.transform_{taxi_type}', key=f'{taxi_type}_transform_file')
    data_date = kwargs['ds']
    partition_folder = f'data_date={data_date}'
    destination_blob_name = f'{dataset_name}/{taxi_type}/{partition_folder}/{taxi_type}_transformed.parquet'

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_conn')
    gcs_hook.upload(bucket_name=GCS_BUCKET, object_name=destination_blob_name, filename=transform_file)
    ti.xcom_push(key=f'{taxi_type}_gcs_file', value=destination_blob_name)