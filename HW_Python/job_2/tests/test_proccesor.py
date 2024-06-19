"""
Tests for request processor file
"""
import os
import shutil
import sys
from pathlib import Path
import json

import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.resolve() / '.env')
print(Path(__name__).resolve())
CORE_DIR = os.environ['CORE_DIR']
TMP_PATH = os.environ['TMP_PATH']

sys.path.append(CORE_DIR)

from post_request_proccesor import post_request_handler, RawDirError


@pytest.fixture
def aux_test_directory():
    """
    Fixture options for pytest
    Used to crate and remove paths for every test
    """


    os.makedirs(TMP_PATH)
    yield
    shutil.rmtree(TMP_PATH)
    print(TMP_PATH)


def aux_folder_creator(raw_dir_date='2022-08-09', stg_dir_date='2022-08-09',
                       record=[{'client': 'Michael Wilkerson',
                               'purchase_date': '2022-08-09',
                               'product': 'Vacuum cleaner',
                               'price': 346}]) -> None:
    """
    asdada
    """
    check_path: str = Path(TMP_PATH)
    raw_dir_arg: Path = check_path / 'raw_dir' / 'sales' / raw_dir_date
    stg_dir_arg: Path = check_path / 'stg_dir' / 'sales' / stg_dir_date
    
    os.makedirs(raw_dir_arg)
    
    with open(raw_dir_arg / f'sales_{raw_dir_date}.json',
              mode='w', encoding='utf-8') as f:
            ## Create test raw file
            json.dump(record, f)

    return raw_dir_arg, stg_dir_arg


def test_trivial_handler(aux_test_directory: None) -> None:
    """
    Test for trivial use case
    """
    
    
    raw_dir_arg, stg_dir_arg = aux_folder_creator()
    print(stg_dir_arg)
        
    post_request_handler(raw_dir_arg, stg_dir_arg)
    
    folder_check: bool = os.path.exists(stg_dir_arg)
    print(os.listdir(stg_dir_arg))
    file_check: bool = len(os.listdir(stg_dir_arg)) == 1\
                       and os.listdir(stg_dir_arg)[0] == 'sales_2022-08-09.avro'

    assert folder_check & file_check


def test_exist_path(aux_test_directory: None) -> None:
    """
    Test for case when path exist
    """

    raw_dir_arg, stg_dir_arg = aux_folder_creator()

    os.makedirs(stg_dir_arg)
    post_request_handler(raw_dir_arg, stg_dir_arg)
    
    folder_check: bool = os.path.exists(stg_dir_arg)
    file_check: bool = len(os.listdir(stg_dir_arg)) == 1\
                       and os.listdir(stg_dir_arg)[0] == 'sales_2022-08-09.avro'

    assert folder_check & file_check


def test_exist_file(aux_test_directory: None) -> None:
    """
    Test for case when path exist with additional files
    """


    raw_dir_arg, stg_dir_arg = aux_folder_creator()

    os.makedirs(stg_dir_arg / 'sales_2022-08-09.avro')
    post_request_handler(raw_dir_arg, stg_dir_arg)
    
    folder_check: bool = os.path.exists(stg_dir_arg)
    file_check: bool = len(os.listdir(stg_dir_arg)) == 1\
                       and os.listdir(stg_dir_arg)[0] == 'sales_2022-08-09.avro'

    assert folder_check & file_check


def test_unexist_raw_file(aux_test_directory: None):
    """
    Test for case of trivial request
    """


    raw_dir_arg, stg_dir_arg = aux_folder_creator()
    os.remove(raw_dir_arg / 'sales_2022-08-09.json')
    
    try:
        post_request_handler(raw_dir_arg, stg_dir_arg)
    except RawDirError as e:
        
        stdiout_check: bool = 'Folder is empty.' in e.message
        assert  stdiout_check
    else:
        assert 0


def test_suspisious_raw_file(aux_test_directory: None, capsys):
    """
    Test for case of trivial request
    """


    raw_dir_arg, stg_dir_arg = aux_folder_creator()
    os.rename(raw_dir_arg / 'sales_2022-08-09.json', raw_dir_arg / '20220809.json')
    
    try:
        post_request_handler(raw_dir_arg, stg_dir_arg)
    except RawDirError as e:
        
        stdiout_check: bool = 'Suspisious file' in e.message
        assert  stdiout_check
    else:
        assert 0


def test_wrong_date_file(aux_test_directory: None):
    """
    Test for case of trivial request
    """


    raw_dir_arg, stg_dir_arg = aux_folder_creator()
    os.rename(raw_dir_arg / 'sales_2022-08-09.json', raw_dir_arg / 'sales_2022-10-09.json')
    print(os.listdir(raw_dir_arg))
    
    try:
        post_request_handler(raw_dir_arg, stg_dir_arg)
    except RawDirError as e:
        
        stdiout_check: bool = 'Suspisious file' in e.message
        assert  stdiout_check
    else:
        assert 0