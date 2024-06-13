"""
Component with input validation funtions
and specific exceptions
"""
from datetime import datetime
import os
from pathlib import Path


class InvalidDateError(Exception):
    """
    Custom exception for invalid date format
    """


    def __init__(self, date: str):
        self.message = 'Invalid date format.\n'\
                        + 'Expected date format: %Y-%m-%d.'\
                        + f'Recieved: {date}.'
        super().__init__(self.message)


class InvalidPathError(Exception):
    """
    Custom exception for invalid date format
    """


    def __init__(self, date: str):
        self.message = 'Invalid path format.\n'\
                       + 'Expected path format: /path/to/my_dir/raw/sales/.\n'\
                       + f'Recieved: {date}.'
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


def date_format_validate(date: str) -> None:
    """
    Check if date input in valid format
    """


    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        raise InvalidDateError(date) from None


def path_format_validate(path: str, date: str) -> None:
    """
    Check if path input in valid format and exist
    """
    

    path_obj = Path(path)
    path_before_date: str = str(path_obj.parent.resolve())
    path_date: str = path_obj.name

    path_cont_pattern: bool = path_before_date.endswith(os.sep.join(['raw', 'sales']))
    path_consistent_date: bool = path_date == date
    
    print(path_obj.parent.resolve(), path_date)
    print(path_cont_pattern, path_consistent_date)

    if not (path_cont_pattern and path_consistent_date):
        raise InvalidPathError(path)

def json_format_validate(json_ob: dict) -> None:
    """
    Check if json input has only two parameters raw_dir and date
    """
    

    keys_list = ['raw_dir', 'date']

    try:
        for key in  json_ob.keys():
            keys_list.remove(key)

        if len(keys_list) != 0:
            raise ValueError
    
    except ValueError:
        raise InvalidJSONError(json_ob) from None
