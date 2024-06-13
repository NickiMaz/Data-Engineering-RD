"""
Script which contain GET request proccessing and sving data to folder
"""
import os
from pathlib import Path
import shutil

from itertools import count
import json

import requests

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.resolve() / '.env')

AUTH_TOKEN = os.environ.get('AUTH_TOKEN', '')

SaleRecord = dict[str, str]


def request_json_from_api(date: str) -> list[SaleRecord]:
    """
    Function for creating GET request to API.
    Creating result structure.
    Also catch requsets lib level exception.
    """

    # Data Structure which accumulate pages of response
    result_list: list = []
    
    # As we dont know number of pages any date
    # we will iterate over the pages until specific error occur
    for page_number in count(1):
        try:
            ## Make GET request to API
            response = requests.get(
                url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
                params={'date': date,
                        'page': page_number,
                        },
                headers={'Authorization': AUTH_TOKEN},
                timeout=27,
                )
            response_json = response.json()
            response.raise_for_status()
        except requests.HTTPError as err:
            ## Check if exception occur
            ## because next page or slaes for date dont exist
            if (response.status_code == 404  and page_number != 1
                and "You requested page that doesn't exist" in response.text):
                ## If pages loaded before this log that process finished
                print('Loading complete')
            
            elif (response.status_code == 404 and page_number == 1
                  and "No data found for date" in response.text):
                ## If sales dont exist for this date just logging
                print('Sales dont exist for this date')

            else:
                ## Otherwise raise exception again
                raise err
            
            ## Exit from cycle in first two cases,
            ## otherwise error raised and script corrupted
            break

        ## Add new page to result structure
        print(f'Add page {page_number} of data')
        result_list.extend(response_json)

    return result_list


def post_request_handler(date: str, raw_dir: str) -> None:
    """
    Main procedure, where data requested and stored on disk
    """


    ## Format raw path
    destination_dir: Path = Path(raw_dir)
    ## Get json with data
    result_list: list[SaleRecord] = request_json_from_api(date)

    ## Create folders only if records exist
    if len(result_list) != 0:

        ## Make folders or delete whethever they exist
        ## Delete only when exist new data
        if os.path.exists(destination_dir):
            print('Delete existed path')
            shutil.rmtree(destination_dir)
            os.makedirs(destination_dir)
        else:
            os.makedirs(destination_dir)

        print('Destination path created')
        
        ## Write result to file
        with open(destination_dir / f'sales_{date}.json',
                  mode='w', encoding='utf-8') as f:
            json.dump(result_list, f)

        print('Writing to file complete')
