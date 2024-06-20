"""
Python module for comfortable importing of parameters in internal format
"""
from pathlib import Path
from airflow.models import Variable

BRONZE_SCHEMA = [
    {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
]

silver_customers_creation_cfg = {
    'query': {
                'query': (Path(__file__).parent \
                          / 'silver_customers_create.sql').read_text(),
                'useLegacySql': False,
                'writeDisposition': 'WRITE_EMPTY',
                'createDisposition': 'CREATE_IF_NEEDED',

                'destinationTable': {
                    'projectId': Variable.get('gcp_project_name'),
                    'datasetId': Variable.get('dataset_silver'),
                    'tableId': Variable.get('table_silver_customers')
                },
            }
}