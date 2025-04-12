
import pandas as pd
from configs.settings import output_path

# Yellow Taxi 
def transform_yellow_data(**kwargs):
    ti = kwargs['ti']
    raw_file = ti.xcom_pull(task_ids='extract_group.load_yellow', key='yellow_raw_file')

    df = pd.read_parquet(raw_file)

    df = df.rename(columns={
        'tpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'PULocationID': 'pickup_locationid',
        'DOLocationID': 'dropoff_locationid'
    })

    df = df[df['passenger_count'] > 0]
    df = df[df['trip_distance'] > 0]

    transform_file = f"{output_path}/yellow_transformed.parquet"
    df.to_parquet(transform_file, index=False)
    ti.xcom_push(key='yellow_transform_file', value=transform_file)

# Green Taxi 
def transform_green_data(**kwargs):
    ti = kwargs['ti']
    raw_file = ti.xcom_pull(task_ids='extract_group.load_green', key='green_raw_file')

    df = pd.read_parquet(raw_file)

    df = df.rename(columns={
        'lpep_pickup_datetime': 'pickup_datetime',
        'lpep_dropoff_datetime': 'dropoff_datetime',
        'PULocationID': 'pickup_locationid',
        'DOLocationID': 'dropoff_locationid'
    })

    df = df[df['passenger_count'] > 0]
    df = df[df['trip_distance'] > 0]

    transform_file = f"{output_path}/green_transformed.parquet"
    df.to_parquet(transform_file, index=False)
    ti.xcom_push(key='green_transform_file', value=transform_file)

# Zone Lookup 
def transform_zone_lookup_data(**kwargs):
    ti = kwargs['ti']
    raw_file = ti.xcom_pull(task_ids='extract_group.load_zone', key='zone_raw_file')

    df = pd.read_csv(raw_file)

    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

    transform_file = f"{output_path}/zone_transformed.parquet"
    df.to_parquet(transform_file, index=False)
    ti.xcom_push(key='zone_transform_file', value=transform_file)