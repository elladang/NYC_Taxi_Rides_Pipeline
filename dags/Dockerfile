FROM apache/airflow:2.3.0-python3.8

WORKDIR /app
USER root

RUN apt-get update && apt-get install -y git


USER airflow

RUN pip install dbt-bigquery==1.5.7

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8080
CMD ["airflow", "webserver"]