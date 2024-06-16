"""
Storage for dags which copy sales from local fs to GSC
"""
import os

from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


with DAG(
    dag_id='process_sales_python_dag',
    description='This DAG extract sales from API and after convert it to avro.\n\
                 Dailly basis on 01:00 UTC.',
    tags=['RD', 'HW_GCS'],
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    
) as dag:

    logical_date_tr = '{{ ds }}'
    target_file = f'sales_{logical_date_tr}.avro'
    
    stg_path: str = os.path.join('/opt/airflow/file_storage/stg/sales', logical_date_tr, target_file)
    
    BUCKET_NAME: str = 'hw_gsp'

    gcs_path: str = os.path.join("sales", "v1")

    copy_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='copy_to_gcs_task',
        src=stg_path,
        dst=os.path.join(gcs_path, '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y/%m/%d") }}', target_file),
        bucket=BUCKET_NAME,
    )

    copy_to_gcs_task
    