"""
Python module for comfortable importing of parameters in internal format
"""
from pathlib import Path
from airflow.models import Variable

BRONZE_SCHEMA = [
    {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'birth_date', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'}
]

silver_user_profiles_creation_cfg = {
            'query': {
                'query': (Path(__file__).parent \
                          / 'silver_user_profiles_create.sql').read_text(),
                'useLegacySql': False,
                'writeDisposition': 'WRITE_EMPTY',
                'createDisposition': 'CREATE_IF_NEEDED',

                'destinationTable': {
                    'projectId': Variable.get('gcp_project_name'),
                    'datasetId': Variable.get('dataset_silver'),
                    'tableId': Variable.get('table_silver_user_profiles')
                },
            }
        }