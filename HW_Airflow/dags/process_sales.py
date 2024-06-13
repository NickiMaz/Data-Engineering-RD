"""
Storage for dags which extract sales from API and convert data to avro format
"""
import os
from datetime import datetime

import requests
from requests import Response
import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.providers.http.hooks.http import HttpHook

BASE_DIR: str = os.environ.get('BASE_DIR')

RAW_DIR: str = os.path.join(BASE_DIR, "raw", "sales")
STG_DIR: str = os.path.join(BASE_DIR, "stg", "sales")


class CustomAirflowException(Exception):

    def __init__(self):
        self.message = 'Something goes wrong, please check the logs'
        super().__init__(self.message)


def request_data_from_api(**kwargs):
    """
    This task make pull request to extract sales from API
    """
    
    
    print('Request for data from api')
    ## Convert default string dt from airflow context tp %Y-%m-%d
    date: str = kwargs['logical_date'].isoformat()
    date: str = datetime.strftime(datetime.fromisoformat(date), '%Y-%m-%d')
    
    print(f'Execution of task for {date}')
    path_req: str = os.path.join(RAW_DIR, date)
    
    resp: Response = requests.post(
            url='http://host.docker.internal:8081',
            json={
                "date": date,
                "raw_dir": path_req,
            }, timeout=30,
        )
    
    if resp.status_code != 201:
        raise CustomAirflowException()


def request_convert_avro(**kwargs):
    """
    This task make pull request to convert json sales files to avro fromat
    """
    
    
    print('Request convert to avro')
    date: str = kwargs['logical_date'].isoformat()
    date: str = datetime.strftime(datetime.fromisoformat(date), '%Y-%m-%d')

    print(f'Execution of task for {date}')
    path_req_raw: str = os.path.join(RAW_DIR, date)
    path_req_stg: str = os.path.join(STG_DIR, date)
    
    resp: Response = requests.post(
            url='http://host.docker.internal:8082',
            json={
                "stg_dir": path_req_stg,
                "raw_dir": path_req_raw,
            }, timeout=30,
        )

    if resp.status_code != 201:
        raise CustomAirflowException()


with DAG(
    dag_id='process_sales_python_dag',
    description="This DAG extract sales from API and after convert it to avro.\n\
                 Dailly basis on 01:00 UTC.",
    tags=['RD', 'HW_Airflow'],

    schedule_interval="0 1 * * *",
    start_date=pendulum.datetime(2022, 8, 8, 2, tz="UTC"),
    end_date=pendulum.datetime(2022, 8, 11, 2, tz="UTC"),
    
    catchup=True,
    
    max_active_runs=1,
    
) as dag:

    extract_data_api = PythonOperator(
        task_id = 'extract_data_api',
        python_callable=request_data_from_api,
    )
    
    convert_to_avro = PythonOperator(
        task_id = 'convert_to_avro',
        python_callable=request_convert_avro,
        depends_on_past=True,
    )

    extract_data_api >> convert_to_avro

    