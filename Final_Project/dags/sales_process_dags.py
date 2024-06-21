'''
Description of procesinng sales content from raw to bronze and silver layers.
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

from sales_process_cfg.sales_process_schemas import (
    BRONZE_SCHEMA,
    silver_sales_creation_cfg
)

with DAG(dag_id='process_sales', schedule=None, catchup=False, max_active_runs=1,
         tags=['RD', 'Final Project']):
    
    start_task = EmptyOperator(task_id='start_task')

    create_sales_bronze_layer_task = BigQueryCreateExternalTableOperator(
        task_id='create_sales_bronze_layer_task',
        destination_project_dataset_table=Variable.get('table_bronze_sales'),
        bucket=Variable.get('raw_bucket_name'),
        source_objects=[Variable.get('path_to_sales')],
        source_format='CSV',
        skip_leading_rows=1,
        schema_fields=BRONZE_SCHEMA,
    )

    create_sales_silver_layer_task = BigQueryInsertJobOperator(
        task_id='create_sales_silver_layer_task',
        location='US',
        configuration=silver_sales_creation_cfg,
    )

    end_task = EmptyOperator(task_id='end_task')

    start_task >> create_sales_bronze_layer_task >> create_sales_silver_layer_task >> end_task