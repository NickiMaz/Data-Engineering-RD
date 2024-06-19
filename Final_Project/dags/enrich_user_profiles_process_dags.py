'''
Description of procesinng enrich_user_profiles tables from silver to golden layers.
GCS -> BigQuery -> BigQuery 
'''
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    )
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

GCP_PROJECT_NAME = 'rd-final-project'

DATASET_SILVER = 'silver_layer'
TABLE_SILVER = 'silver_user_profiles'

DATASET_GOLDEN = 'golden_layer'
TABLE_GOLDEN = 'golden_customers_enriched'

with open(Path(__file__).parent / 'enrich_user_profiles_process_cfg' / 'golden_layer_create.sql', 'r') as f:
    GOLDEN_LAYER_CREATION_QUERY = f.read()

with open(Path(__file__).parent / 'enrich_user_profiles_process_cfg' / 'golden_enrichment_customers.sql', 'r') as f:
    GOLDEN_ENRICHMENT_CUSTOMERS_QUERY = f.read()

with open(Path(__file__).parent / 'enrich_user_profiles_process_cfg' / 'analysis_query.sql', 'r') as f:
    ANALYSIS_QUERY = f.read()

with DAG(dag_id='process_enrichment_user_profiles', schedule=None, catchup=False, max_active_runs=1,
         tags=['RD', 'Final Project']):
    
    start_task = EmptyOperator(task_id='start_task')

    golden_layer_customers_creation_task = BigQueryInsertJobOperator(
        task_id='golden_layer_customers_creation_task',
        location='US',
        configuration={
            'query': {
                'query': GOLDEN_LAYER_CREATION_QUERY,
                'useLegacySql': False,
                'writeDisposition': 'WRITE_EMPTY',
                'createDisposition': 'CREATE_IF_NEEDED',

                'destinationTable': {
                    'projectId': GCP_PROJECT_NAME,
                    'datasetId': DATASET_GOLDEN,
                    'tableId': TABLE_GOLDEN
                },
            }
        },
    )

    golden_layer_customers_enrichment_task = BigQueryInsertJobOperator(
        task_id='golden_layer_customers_enrichment_task',
        location='US',
        configuration={
            'query': {
                'query': GOLDEN_ENRICHMENT_CUSTOMERS_QUERY,
                'useLegacySql': False,
            }
        },
    )

    def print_analysis_query():
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        df = bq_hook.get_pandas_df(sql=ANALYSIS_QUERY, dialect='standard')
        
        print(df)

    print_analysis_query_task = PythonOperator(
        task_id='print_analysis_query_task',
        python_callable=print_analysis_query
    )

    end_task = EmptyOperator(task_id='end_task')

    start_task >> golden_layer_customers_creation_task >>\
          golden_layer_customers_enrichment_task >> print_analysis_query_task >> end_task