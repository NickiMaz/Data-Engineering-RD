"""
Tests for request processor file
"""
import os
import shutil
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.resolve() / '.env')

CORE_DIR = os.environ['CORE_DIR']
TMP_PATH = os.environ['TMP_PATH']

sys.path.append(CORE_DIR)

from post_request_proccesor import post_request_handler, request_json_from_api


@pytest.fixture
def aux_test_directory():
    """
    Fixture options for pytest
    Used to crate and remove paths for every test
    """


    os.makedirs(TMP_PATH)
    yield
    shutil.rmtree(TMP_PATH)


def test_trivial_handler(aux_test_directory: None) -> None:
    """
    Test for trivial use case
    """


    check_path: str = Path(TMP_PATH)
        
    post_request_handler('2022-08-09', TMP_PATH)
    
    folder_check: bool = os.path.exists(check_path)
    file_check: bool = len(os.listdir(check_path)) == 1\
                       and os.listdir(check_path)[0] == 'sales_2022-08-09.json'

    assert folder_check & file_check


def test_exist_path(aux_test_directory: None) -> None:
    """
    Test for case when path exist
    """


    check_path: str = Path(TMP_PATH)

    post_request_handler('2022-08-09', TMP_PATH)

    folder_check: bool = os.path.exists(check_path)        
    file_check: bool = len(os.listdir(check_path)) == 1\
                       and os.listdir(check_path)[0] == 'sales_2022-08-09.json'

    assert folder_check & file_check


def test_exist_file(aux_test_directory: None) -> None:
    """
    Test for case when path exist with additional files
    """


    check_path: str = Path(TMP_PATH)
    os.makedirs(check_path / 'test_provoke.txt')

    post_request_handler('2022-08-09', TMP_PATH)

    folder_check: bool = os.path.exists(check_path)        
    file_check: bool = len(os.listdir(check_path)) == 1\
                       and os.listdir(check_path)[0] == 'sales_2022-08-09.json'
    
    assert folder_check & file_check


def test_trivial_request(capsys):
    """
    Test for case of trivial request
    """


    result_check = len(request_json_from_api('2022-08-09')) != 0
    captured = capsys.readouterr()
    stdiout_check = 'Loading complete' in captured.out

    assert stdiout_check & result_check


def test_unexisted_date_request(capsys):
    """
    Test for case when sales dont exist for request
    """

    
    result_check = len(request_json_from_api('2024-01-01')) == 0
    captured = capsys.readouterr()
    stdiout_check = 'Sales dont exist for this date' in captured.out
    
    assert  stdiout_check & result_check
