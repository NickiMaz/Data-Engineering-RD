'''
Description of procesinng enrich_user_profiles tables from silver to golden layers.
GCS -> BigQuery -> BigQuery 
'''
from pathlib import Path

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    )
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from enrich_user_profiles_process_cfg.enrich_user_profiles_process_schemas import (
    creation_golden_cfg,
    enrichment_user_profilies_cfg,
    analysis_query_sql   
)


with DAG(dag_id='process_enrichment_user_profiles', schedule=None, catchup=False, max_active_runs=1,
         tags=['RD', 'Final Project']):
    
    start_task = EmptyOperator(task_id='start_task')

    golden_layer_customers_creation_task = BigQueryInsertJobOperator(
        task_id='golden_layer_customers_creation_task',
        location='US',
        configuration=creation_golden_cfg,
    )

    golden_layer_customers_enrichment_task = BigQueryInsertJobOperator(
        task_id='golden_layer_customers_enrichment_task',
        location='US',
        configuration=enrichment_user_profilies_cfg,
    )

    def print_analysis_query():
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        df = bq_hook.get_pandas_df(sql=analysis_query_sql,
                                    dialect='standard')
        
        print(df)

    print_analysis_query_task = PythonOperator(
        task_id='print_analysis_query_task',
        python_callable=print_analysis_query
    )

    end_task = EmptyOperator(task_id='end_task')

    start_task >> golden_layer_customers_creation_task >>\
          golden_layer_customers_enrichment_task >> print_analysis_query_task >> end_task