import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import os
import time
import json
from urllib.request import Request, urlopen
import shutil
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# # Configuration
# dataset_name = 'dell_monitors'
# staging_table_name = 'dell_monitors_staging'
# mart_table_name = 'dell_monitors_mart'
# outputpath = '/var/tmp'
# GCS_BUCKET = 'dell_monitors_raw_data'
# project_id = 'trangdang'
# crawl_url = 'https://www.tnc.com.vn/man-hinh-dell.html'

# # EXTRACT: Crawling
# def crawl_dell_monitors_data(**kwargs):
#     service = Service(executable_path=ChromeDriverManager().install())
#     options = Options()
#     options.add_argument('--headless')  # Run in headless mode for Airflow
#     driver = webdriver.Chrome(service=service, options=options)

#     driver.maximize_window()
#     driver.get(crawl_url)

#     container_list_product = driver.find_element(by=By.ID, value="list-product")
#     while True:
#         try:
#             tag_button_load_more = driver.find_element(by=By.XPATH, value="//div[@class='load-more']/button")
#             tag_button_load_more.click()
#             time.sleep(1.5)
#         except:
#             break

#     container_list_product = driver.find_element(By.ID, "list-product")
#     list_link = [
#         item.get_attribute("href")
#         for item in container_list_product.find_elements(By.XPATH, "//div[@class='product hover']//a[@class='effect']")
#     ]
#     driver.close()

#     driver_detail = webdriver.Chrome(service=service, options=options)
#     driver_detail.maximize_window()
#     items = {
#         'product_name': [],
#         'product_price': [],
#         'image_links': [],
#         'short_desc_tag': []
#     }

#     for link in list_link:
#         link = link.strip("\n")
#         driver_detail.get(link)
#         time.sleep(0.5)

#         product_name_tag = driver_detail.find_element(by=By.CLASS_NAME, value="post-title")
#         product_name = product_name_tag.get_attribute("textContent")

#         product_price_tag = driver_detail.find_element(by=By.XPATH, value="//div[@class='box-price']//span[@class='price']")
#         product_price = product_price_tag.text.strip("đ").replace(",", "")
#         product_price = int(product_price) if product_price.isdigit() else 0

#         image_tag = driver_detail.find_element(by=By.XPATH, value="//div[@class ='item hover slick-slide slick-current slick-active']//img")
#         image_links = image_tag.get_attribute('src')

#         short_desc_tag = driver_detail.find_elements(by=By.XPATH, value="//ul[@class='option-top']/li")
#         short_desc_str = ''.join(short_desc.text for short_desc in short_desc_tag)

#         items['product_name'].append(product_name)
#         items['product_price'].append(product_price)
#         items['image_links'].append(image_links)
#         items['short_desc_tag'].append(short_desc_str)

#     driver_detail.close()

#     os.makedirs("output/thanh-nhan-imgs-dell", exist_ok=True)
#     headers = {'User-Agent': 'Mozilla/5.0'}
#     image_list = items['image_links']
#     image_paths = []

#     for i in image_list:
#         try:
#             if not i or not i.startswith("http"):
#                 continue
#             file_name = i.split('/')[-1]
#             image_path = f"output/thanh-nhan-imgs-dell/{file_name}"
#             req = Request(i, headers=headers)
#             with urlopen(req, timeout=10) as response, open(image_path, 'wb') as out_file:
#                 shutil.copyfileobj(response, out_file)
#             image_paths.append(image_path)
#             time.sleep(1)
#         except Exception as e:
#             logger.warning(f"Could not download image {i}: {str(e)}")

#     output_data = []
#     for i in range(len(items['product_name'])):
#         output_data.append({
#             "product_name": items['product_name'][i],
#             "product_price": items['product_price'][i],
#             "image_link": items['image_links'][i],
#             "short_desc": items['short_desc_tag'][i]
#         })

#     json_path = os.path.join(outputpath, "tnc_dell_monitors.json")
#     with open(json_path, 'w', encoding='utf-8') as f:
#         json.dump(output_data, f, ensure_ascii=False, indent=4)

#     kwargs['ti'].xcom_push(key='raw_file', value=json_path)
#     kwargs['ti'].xcom_push(key='image_paths', value=image_paths)

# # TRANSFORM
# def transform_data(**kwargs):
#     ti = kwargs['ti']
#     raw_file = ti.xcom_pull(task_ids='crawl_dell_monitors_data', key='raw_file')
#     df = pd.read_json(raw_file)
#     transformed_file = f"{outputpath}/dell_transformed_data.parquet"
#     df.to_parquet(transformed_file, index=False)
#     ti.xcom_push(key='transformed_file', value=transformed_file)

# # UPLOAD TO GCS: Data
# def upload_to_gcs(**kwargs):
#     ti = kwargs['ti']
#     transform_file = ti.xcom_pull(task_ids='transform_data', key='transformed_file')
#     data_date = kwargs['ds']
#     partion_folder = f'data_date={data_date}'
#     destination_blob_name = f'{dataset_name}/{partion_folder}/transformed_data.parquet'

#     gcs_hook = GCSHook(gcp_conn_id='google_cloud_conn')
#     gcs_hook.upload(bucket_name=GCS_BUCKET, object_name=destination_blob_name, filename=transform_file)
#     ti.xcom_push(key='gcs_file', value=destination_blob_name)

# # UPLOAD TO GCS: Images
# def upload_images_to_gcs(**kwargs):
#     ti = kwargs['ti']
#     image_paths = ti.xcom_pull(task_ids='crawl_dell_monitors_data', key='image_paths')
#     data_date = kwargs['ds']
#     partion_folder = f'data_date={data_date}'
    
#     gcs_hook = GCSHook(gcp_conn_id='google_cloud_conn')
#     for image_path in image_paths:
#         file_name = os.path.basename(image_path)
#         destination_blob_name = f'images/{partion_folder}/{file_name}'
#         gcs_hook.upload(bucket_name=GCS_BUCKET, object_name=destination_blob_name, filename=image_path)

# # DAG Definition
# with DAG(
#     dag_id='dell_monitors_crawl_to_bigquery',
#     start_date=datetime(2025, 3, 21),
#     end_date=datetime(2025, 3, 23),
#     schedule_interval='@daily',
#     catchup=False,
# ) as dag:

#     crawl_data_task = PythonOperator(
#         task_id='crawl_dell_monitors_data',
#         python_callable=crawl_dell_monitors_data,
#         provide_context=True
#     )

#     transform_data_task = PythonOperator(
#         task_id='transform_data',
#         python_callable=transform_data,
#         provide_context=True
#     )

#     upload_to_gcs_task = PythonOperator(
#         task_id='upload_to_gcs',
#         python_callable=upload_to_gcs,
#         provide_context=True
#     )

#     upload_images_to_gcs_task = PythonOperator(
#         task_id='upload_images_to_gcs',
#         python_callable=upload_images_to_gcs,
#         provide_context=True
#     )

#     load_to_bigquery_task = GCSToBigQueryOperator(
#         task_id="load_to_bigquery",
#         bucket=GCS_BUCKET,
#         source_objects=[f'{dataset_name}/data_date={{ds}}/transformed_data.parquet'],
#         destination_project_dataset_table=f'{project_id}.{dataset_name}.{staging_table_name}',
#         source_format="PARQUET",
#         write_disposition="WRITE_APPEND",
#         gcp_conn_id='google_cloud_conn',
#     )

#     crawl_data_task >> transform_data_task >> [upload_to_gcs_task, upload_images_to_gcs_task]
#     upload_to_gcs_task >> load_to_bigquery_task 