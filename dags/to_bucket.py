from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import pandas  as pd
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
with DAG(
    dag_id = "to_bucket",
    start_date = datetime(2024, 6, 20),
    schedule = "@once",
    catchup = False,
) as dag:
    
    files = ['bookings.csv','hosts.csv','listings.csv']

    for file in files: 
        
        load_to_gcs = LocalFilesystemToGCSOperator(
            task_id = f"load_to_gcs_{file}",
            src = f"include/{file}",
            dst = f"raw_airbnb/{file}",
            bucket = "airflow_dump_1",
            gcp_conn_id = "gcp_standard"
        )
    file_config = [ {'file':'raw_airbnb/bookings.csv',
                     'dest':'bookings_raw'},
                    {'file':'raw_airbnb/hosts.csv',
                     'dest':'hosts_raw'},
                    {'file':'raw_airbnb/listings.csv',
                     'dest':'listings_raw'}
                   ]
    for config in file_config:
        load_to_bg = GCSToBigQueryOperator(
            task_id = f"load_{config['dest']}_to_bg",
            bucket = "airflow_dump_1",
            source_objects = [config['file']],
            destination_project_dataset_table = f"projects-482900.airbnb_staging.{config['dest']}",
            source_format = "CSV",
            skip_leading_rows = 1,
            write_disposition = "WRITE_TRUNCATE",
            autodetect=True,
            gcp_conn_id = "gcp_standard"
        )
        
        load_to_gcs >> load_to_bg


