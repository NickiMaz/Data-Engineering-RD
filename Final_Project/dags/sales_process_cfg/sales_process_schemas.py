"""
Python module for comfortable importing of parameters in internal format
"""
from pathlib import Path
from airflow.models import Variable

BRONZE_SCHEMA = [
    {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'},
]

silver_sales_creation_cfg = {
    'query': {
                'query': (Path(__file__).parent \
                           / 'silver_sales_create.sql').read_text(),
                'useLegacySql': False,
                'writeDisposition': 'WRITE_EMPTY',
                'createDisposition': 'CREATE_IF_NEEDED',

                'destinationTable': {
                    'projectId': Variable.get('gcp_project_name'),
                    'datasetId': Variable.get('dataset_silver'),
                    'tableId': Variable.get('table_silver_sales')
                },
                'time_partitioning': {'field': 'purchase_date', 'type': 'DAY'},
            }
}