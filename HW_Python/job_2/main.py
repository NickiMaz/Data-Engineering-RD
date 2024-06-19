"""
Flask handler for POST request server for job 2 (JSON -> Avro) 
"""
from flask import Flask
from flask import request, make_response

from post_request_proccesor import post_request_handler
from post_request_validator import path_format_validate, json_format_validate

app = Flask(__name__)

@app.post('/')
def post_data() -> str:
    """
    Function which handle POST requests
    """
    
    json_ob = request.json
    json_format_validate(json_ob)
    
    req_raw_dir: str = json_ob['raw_dir']
    path_format_validate(req_raw_dir, 'raw')

    req_stg_dir: str = json_ob['stg_dir']
    path_format_validate(req_stg_dir, 'stg')

    post_request_handler(req_raw_dir, req_stg_dir)

    return make_response('Succes job_2', 201)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082, debug=True)