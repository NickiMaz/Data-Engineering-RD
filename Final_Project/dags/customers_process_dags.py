'''
Description of procesinng customers tables from raw to bronze and silver layers.
GCS -> BigQuery -> BigQuery 
'''
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
    )

from airflow.operators.empty import EmptyOperator

from customers_process_cfg.customers_process_schemas import BRONZE_SCHEMA

GCP_PROJECT_NAME = 'rd-final-project'
RAW_BUCKET_NAME = 'file_storage_raw'

DATASET_BRONZE = 'bronze_layer'
TABLE_BRONZE = 'bronze_customers'
PATH_TO_SOURCE = 'data/customers/*.csv'

DATASET_SILVER = 'silver_layer'
TABLE_SILVER = 'silver_customers'

with open(Path(__file__).parent / 'customers_process_cfg' / 'silver_customers_create.sql', 'r') as f:
    SILVER_CUSTOMERS_QUERY = f.read()

with DAG(dag_id='process_customers', schedule=None, catchup=False, max_active_runs=1,
         tags=['RD', 'Final Project']):
    
    start_task = EmptyOperator(task_id='start_task')

    create_customers_bronze_layer_task = BigQueryCreateExternalTableOperator(
        task_id='create_customers_bronze_layer_task',
        destination_project_dataset_table=f'{DATASET_BRONZE}.{TABLE_BRONZE}',
        bucket=RAW_BUCKET_NAME,
        source_objects=[PATH_TO_SOURCE],
        source_format='CSV',
        skip_leading_rows=1,
        schema_fields=BRONZE_SCHEMA,
    )

    create_customers_silver_layer_task = BigQueryInsertJobOperator(
        task_id='create_customers_silver_layer_task',
        location='US',
        configuration={
            'query': {
                'query': SILVER_CUSTOMERS_QUERY,
                'useLegacySql': False,
                'writeDisposition': 'WRITE_EMPTY',
                'createDisposition': 'CREATE_IF_NEEDED',

                'destinationTable': {
                    'projectId': GCP_PROJECT_NAME,
                    'datasetId': DATASET_SILVER,
                    'tableId': TABLE_SILVER
                },
            }
        },
    )

    end_task = EmptyOperator(task_id='end_task')

    start_task >> create_customers_bronze_layer_task >> create_customers_silver_layer_task >> end_task