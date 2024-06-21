"""
Python module for comfortable importing of parameters in internal format
"""
from pathlib import Path
from airflow.models import Variable


creation_golden_cfg = {
    'query': {
                'query': (Path(__file__).parent \
                          / 'golden_layer_create.sql').read_text(),
                'useLegacySql': False,
                'writeDisposition': 'WRITE_EMPTY',
                'createDisposition': 'CREATE_IF_NEEDED',

                'destinationTable': {
                    'projectId': Variable.get('gcp_project_name'),
                    'datasetId': Variable.get('dataset_golden'),
                    'tableId': Variable.get('table_golden')
                },
            }
        }

enrichment_user_profilies_cfg = {
    'query': {'query': (Path(__file__).parent \
                            / 'golden_enrichment_customers.sql').read_text(),
                        'useLegacySql': False,
                        }
}

analysis_query_sql = (Path(__file__).parent / 'analysis_query.sql').read_text()