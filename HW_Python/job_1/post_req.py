import os
import requests
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.resolve() / '.env')
BASE_DIR = os.environ.get('BASE_DIR', '.')

if __name__ == '__main__':
    resp = requests.post(
            url='http://localhost:8081/',
            json={
                "date": "2022-08-09",
                "raw_dir": BASE_DIR,
            }, timeout=30,
        )