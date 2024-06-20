'''
Description of procesinng user_profiles tables from raw to bronze and silver layers.
GCS -> BigQuery -> BigQuery 
'''
from pathlib import Path

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
    )

from airflow.operators.empty import EmptyOperator

from user_profiles_process_cfg.user_profiles_process_schemas import (
    BRONZE_SCHEMA,
    silver_user_profiles_creation_cfg
)

with DAG(dag_id='process_user_profiles', schedule=None, catchup=False, max_active_runs=1,
         tags=['RD', 'Final Project']):
    
    start_task = EmptyOperator(task_id='start_task')

    create_user_profiles_bronze_layer_task = BigQueryCreateExternalTableOperator(
        task_id='create_user_profiles_bronze_layer_task',
        destination_project_dataset_table=Variable.get('table_bronze_user_profiles'),
        bucket=Variable.get('raw_bucket_name'),
        source_objects=[Variable.get('path_to_user_profiles')],
        source_format='NEWLINE_DELIMITED_JSON',
        skip_leading_rows=1,
        schema_fields=BRONZE_SCHEMA,
    )

    create_user_profiles_silver_layer_task = BigQueryInsertJobOperator(
        task_id='create_user_profiles_silver_layer_task',
        location='US',
        configuration=silver_user_profiles_creation_cfg,
    )

    end_task = EmptyOperator(task_id='end_task')

    start_task >> create_user_profiles_bronze_layer_task >> create_user_profiles_silver_layer_task >> end_task