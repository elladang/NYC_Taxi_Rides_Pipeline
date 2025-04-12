GCS_BUCKET = 'ny_taxi_raw_data'
dataset_name = 'ny_taxi'
project_id = 'trangdang'

output_path = '/var/tmp'
GCP_CONN_ID = 'google_cloud_conn' 
source_urls = {
    "yellow": 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet',
    "green": 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet',
    "zone": 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
}