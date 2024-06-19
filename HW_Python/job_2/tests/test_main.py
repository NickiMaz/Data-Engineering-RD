"""
Tests for post parameter
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.resolve() / '.env')

CORE_DIR = os.environ['CORE_DIR']
TMP_PATH = os.environ['TMP_PATH']

sys.path.append(CORE_DIR)

from post_request_validator import (json_format_validate, path_format_validate)

from post_request_validator import (InvalidPathError, InvalidJSONError)


def test_unconsistent_stg_path_format() -> None:
    """
    Test stg path validator
    """
    path_test_1 = '/path/to/my_dir/stg/sales/2022-08-09'
    path_test_2 = '/path/to/my_dir/stg/exp/2022.08.09'
    path_test_3 = '/path/to/my_dir/raw/sales/2022-08-09'

    for path_test in [path_test_1, path_test_2, path_test_3]: 
        try:
            path_format_validate(path_test, 'stg')
        except InvalidPathError as e:
            assert 'Invalid path format' in e.message

def test_unexist_raw_path() -> None:
    """
    Test exception if raw dir doesnt exist
    """


    path_test = '/path/to/my_dir/raw/sales/2022-08-09'
    
    assert os.path.exists(path_test) != 1


def test_invalid_json_format() -> None:
    """
    Test for trivial path format
    """


    try:
        json_format_validate({})
    except InvalidJSONError as e:
        assert 'Invalid json format' in e.message


def test_dublicate_json_format() -> None:
    """
    Test for trivial path format
    """


    try:
        json_format_validate({'raw_dir': TMP_PATH, 'stg_dir': TMP_PATH, 'raw_dir':  TMP_PATH})
    except InvalidJSONError as e:
        assert 'Invalid json format' in e.message


def test_different_json_format() -> None:
    """
    Test for trivial path format
    """


    try:
        json_format_validate({'data': TMP_PATH, 'stg_dir':  TMP_PATH})
    except InvalidJSONError as e:
        assert 'Invalid json format' in e.message
