"""
Script which contain converting from json format and storing in avro format
"""
import os
from pathlib import Path
import shutil

import re
import json

from fastavro import parse_schema, writer

class RawDirError(Exception):
    """
    Custom exception for invalid date format
    """


    def __init__(self, msg: str =''):
        self.message = 'Trouble with raw_dir. '\
                       + msg
        super().__init__(self.message)


SaleRecord = dict[str, str]

SCHEMA = {
    'doc': 'Sales format',
    'name': 'Sales',
    'type': 'record',
    'fields': [
        {'name': 'client', 'type': 'string'},
        {'name': 'purchase_date', 'type': 'string'},
        {'name': 'product', 'type': 'string'},
        {'name': 'price', 'type': 'int'},
    ],
}
PARSED_SCHEMA = parse_schema(SCHEMA)

def post_request_handler(raw_dir: str, stg_dir: str) -> None:
    """
    Main procedure, which load to memory json files and stores as avro
    """


    ## Get set of files to read
    ## Create folders only if records exist    
    raw_dir_source: Path = Path(raw_dir)
    date_raw: str = raw_dir_source.name
    file_list: list[str] = os.listdir(raw_dir_source)

    if len(file_list) == 0:
        raise RawDirError('Folder is empty.')

    result_list: list[SaleRecord] = []

    for file in file_list:
        if re.fullmatch(f'sales_{date_raw}' + r'(\.|\_\d{0,2}\.)json', file) is None:
            raise RawDirError(f'Suspisious file {file} in raw_dir.')

        with open(raw_dir_source / file, 'r', encoding='utf-8') as f:
            records_reader = json.load(f)
            for record in records_reader:
                result_list.append(record)

    print('Loading from json files complete')

    stg_dir_dest: Path = Path(stg_dir)
    date_stg: str = stg_dir_dest.name
    dest_stg_file: str = f'sales_{date_stg}.avro'

    if os.path.exists(stg_dir_dest):
        print('Delete existed path')
        shutil.rmtree(stg_dir_dest)
        os.makedirs(stg_dir_dest)
    else:
        os.makedirs(stg_dir_dest)

    print('Destination path created')

    with open(stg_dir_dest / dest_stg_file, 'wb') as f:
        writer(f, PARSED_SCHEMA, result_list)
    
    print('Writing to avro file complete')    
