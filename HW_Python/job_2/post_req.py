import os
import requests
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.resolve() / '.env')
RAW_DIR = os.environ.get('RAW_DIR', '.')
STG_DIR = os.environ.get('STG_DIR', '.')

if __name__ == '__main__':
    resp = requests.post(
            url='http://localhost:8082/',
            json={
                "stg_dir": STG_DIR,
                "raw_dir": RAW_DIR,
            }, timeout=30,
        )