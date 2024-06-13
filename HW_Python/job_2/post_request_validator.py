"""
Component with input validation funtions
and specific exceptions
"""
import os
from datetime import datetime
from pathlib import Path


class InvalidPathError(Exception):
    """
    Custom exception for invalid date format
    """


    def __init__(self, msg: str):
        self.message = 'Invalid path format.\n'\
                       + 'Expected path format: /path/to/my_dir/raw/sales/.\n'\
                       + msg
        super().__init__(self.message)


class InvalidJSONError(Exception):
    """
    Custom exception for invalid json format
    """


    def __init__(self, json_ob: str):
        self.message = 'Invalid json format.\n'\
                       + 'Expected date format: {{date: key, raw_dir: key}}.'\
                       + f'Recieved: {json_ob}.'
        super().__init__(self.message)




def path_format_validate(path: str, stage: 'str') -> None:
    """
    Check if path input in valid format and exist
    """
    if stage == 'raw':
        if not os.path.exists(path):
            raise InvalidPathError('Raw path doesnt exist.')
    
    if stage == 'stg':
        path_name_standard = Path(path)
        
        date_path: Path = path_name_standard.resolve()
        date_check: bool = True

        try:
            datetime.strptime(date_path.name, '%Y-%m-%d')
        except:
            date_check = False
        
        sales_path: Path = path_name_standard.parent
        sales_path_check: bool = sales_path.name == 'sales'
        
        stg_path: Path = sales_path.parent
        stg_path_check: bool = stg_path.name == 'stg'
        
        if not (stg_path_check and sales_path_check and date_check):
            raise InvalidPathError(path)

def json_format_validate(json_ob: dict) -> None:
    """
    Check if json input has only two parameters raw_dir and date
    """
    

    keys_list = ['raw_dir', 'stg_dir']

    try:
        for key in  json_ob.keys():
            keys_list.remove(key)

        if len(keys_list) != 0:
            raise ValueError
    
    except ValueError:
        raise InvalidJSONError(json_ob) from None
