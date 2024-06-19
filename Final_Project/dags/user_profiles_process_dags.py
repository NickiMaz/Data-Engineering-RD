'''
Description of procesinng user_profiles tables from raw to bronze and silver layers.
GCS -> BigQuery -> BigQuery 
'''
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
    )

from airflow.operators.empty import EmptyOperator

from user_profiles_process_cfg.user_profiles_process_schemas import BRONZE_SCHEMA

GCP_PROJECT_NAME = 'rd-final-project'
RAW_BUCKET_NAME = 'file_storage_raw'

DATASET_BRONZE = 'bronze_layer'
TABLE_BRONZE = 'bronze_user_profilies'
PATH_TO_SOURCE = 'data/user_profiles/*.csv'

DATASET_SILVER = 'silver_layer'
TABLE_SILVER = 'silver_user_profiles'

with open(Path(__file__).parent / 'user_profiles_process_cfg' / 'silver_user_profiles_create.sql', 'r') as f:
    SILVER_USER_PROFILIES_QUERY = f.read()

with DAG(dag_id='process_user_profiles', schedule=None, catchup=False, max_active_runs=1,
         tags=['RD', 'Final Project']):
    
    start_task = EmptyOperator(task_id='start_task')

    create_user_profiles_bronze_layer_task = BigQueryCreateExternalTableOperator(
        task_id='create_user_profiles_bronze_layer_task',
        destination_project_dataset_table=f'{DATASET_BRONZE}.{TABLE_BRONZE}',
        bucket=RAW_BUCKET_NAME,
        source_objects=[PATH_TO_SOURCE],
        source_format='NEWLINE_DELIMITED_JSON',
        skip_leading_rows=1,
        schema_fields=BRONZE_SCHEMA,
    )

    create_user_profiles_silver_layer_task = BigQueryInsertJobOperator(
        task_id='create_user_profiles_silver_layer_task',
        location='US',
        configuration={
            'query': {
                'query': SILVER_USER_PROFILIES_QUERY,
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

    start_task >> create_user_profiles_bronze_layer_task >> create_user_profiles_silver_layer_task >> end_task