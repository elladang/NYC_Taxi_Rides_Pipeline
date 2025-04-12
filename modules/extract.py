import requests
from configs.settings import source_urls, output_path

def load_data_from_api(taxi_type, **kwargs):
    url = source_urls[taxi_type]
    ext = '.parquet' if url.endswith('.parquet') else '.csv'
    raw_file = f"{output_path}/{taxi_type}_raw{ext}"
    response = requests.get(url)
    with open(raw_file, "wb") as f:
        f.write(response.content)
    kwargs['ti'].xcom_push(key=f'{taxi_type}_raw_file', value=raw_file)
