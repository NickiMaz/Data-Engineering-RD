"""
Flask handler for POST request server for job 1 (API -> Disk: JSON) 
"""
from flask import Flask
from flask import request, make_response

from post_request_proccesor import post_request_handler
from post_request_validator import (date_format_validate, path_format_validate,
                                    json_format_validate)

app = Flask(__name__)

@app.post('/')
def post_data() -> str:
    """
    Function which handle POST requests
    """
    
    json_ob = request.json
    json_format_validate(json_ob)

    request_date: str = json_ob['date']
    date_format_validate(request_date)

    req_raw_dir: str = json_ob['raw_dir']
    path_format_validate(req_raw_dir, request_date)

    post_request_handler(request_date, req_raw_dir)

    return make_response('Succes job_1', 201)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081, debug=True)