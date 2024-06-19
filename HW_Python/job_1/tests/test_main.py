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

from post_request_validator import (date_format_validate, json_format_validate,
                                    path_format_validate)

from post_request_validator import (InvalidDateError, InvalidPathError,
                                    InvalidJSONError)

def test_trivial_date_format() -> None:
    """
    Test for trivial date format
    """

    date_format_validate('2023-03-07')
    assert 1


def test_non_date_format() -> None:
    """
    Test for nondate format
    """

    try:
        date_format_validate('adsffsfdsf')
    except InvalidDateError as e:
        assert 'Invalid date format' in e.message


def test_different_date_format() -> None:
    """
    Test for nondate format
    """    


    try:
        date_format_validate('2023.03.07')
    except InvalidDateError as e:
        assert 'Invalid date format' in e.message


def test_trivial_path_format() -> None:
    """
    Test for trivial path format
    """

    
    path_format_validate(TMP_PATH, '2022-08-09')
    assert 1


def test_unconsistent_date_path_format() -> None:
    """
    Test for nondate format
    """


    try:
        path_format_validate('/path/to/my_dir/raw/sales/2022.08.09', '2022-08-10')
        path_format_validate('/path/to/my_dir/raw/sales/2022.08.10', '2022-08-10')
    except InvalidPathError as e:
        assert 'Invalid path format' in e.message


def test_trivial_json_format() -> None:
    """
    Test for trivial path format
    """


    json_format_validate({'date': '2023-03-07', 'raw_dir': CORE_DIR})
    assert 1


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
        json_format_validate({'date': '2023-03-07', 'date': '2023-03-06', 'raw_dir':  TMP_PATH})
    except InvalidJSONError as e:
        assert 'Invalid json format' in e.message


def test_different_json_format() -> None:
    """
    Test for trivial path format
    """


    try:
        json_format_validate({'data': '2023-03-07', 'stg_dir':  TMP_PATH})
    except InvalidJSONError as e:
        assert 'Invalid json format' in e.message
