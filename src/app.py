import os
import emr
import requests
import logging
import validations
from flask import Flask, url_for, request, make_response, abort


# BioAPI version
VERSION = '0.1.3'

app = Flask(__name__)


@app.get("/")
def index():
    return f"<h1>Multiomix AWS EMR integration v{VERSION}</h1>"


@app.post("/job")
def schedule_job():
    request_data = request.get_json()
    if not validations.schedule_request_is_valid(request_data):
        logging.warning('Invalid data received:')
        logging.warning(request_data)
        abort(400)

    emr_response = emr.schedule(
        request_data["name"],
        request_data["algorithm"],
        request_data["entrypoint_arguments"]
    )

    if emr_response is None:
        abort(500)

    resp = make_response({"id": emr_response["id"]}, 201)
    resp.headers['Location'] = url_for('get_job', job_id=emr_response["id"])
    resp.headers['Content-Type'] = "application/json; charset=utf-8"

    return resp


@app.get("/job/<job_id>")
def get_job(job_id: str):
    obj = __get_job(job_id)
    if obj is None:
        abort(404)
    resp = make_response(obj, 200)
    resp.headers['Content-Type'] = "application/json; charset=utf-8"

    return resp

def __get_job(job_id) -> object:
    emr_response = emr.get(job_id)
    if emr_response is None:
        return None
    
    return {
        "id": emr_response["jobRun"]["id"],
        "createdAt": emr_response["jobRun"]["createdAt"].isoformat(' '),
        "finishedAt": emr_response["jobRun"]["finishedAt"].isoformat(' ') if 'finishedAt' in emr_response['jobRun'] else None,
        "name": emr_response["jobRun"]["name"],
        "state": emr_response["jobRun"]["state"],
        "stateDetails": emr_response["jobRun"]["stateDetails"] if 'stateDetails' in emr_response['jobRun'] else None,
    }

@app.delete("/job/<job_id>")
def cancel_job(job_id: str):
    emr_response = emr.cancel(job_id)
    if emr_response is None:
        abort(409)

    resp = make_response({"id": emr_response["id"]}, 200)
    resp.headers['Location'] = url_for('get_job', job_id=emr_response["id"])
    resp.headers['Content-Type'] = "application/json; charset=utf-8"

    return resp


@app.patch("/job/<job_id>")
def change_status_job(job_id: str):
    resp = make_response({"id": job_id}, 204)
    resp.headers['Location'] = url_for('get_job', job_id=job_id)
    resp.headers['Content-Type'] = "application/json; charset=utf-8"
    app.logger.info("Job id: '{id}' is now in '{state}' state".format(id=job_id, state=request.json.get("state", None)))
    body=__get_job(job_id)
    
    try:
        requests.post(os.getenv("MULTIOMIX_URL"), json=body)
    except requests.exceptions.ConnectionError as err:
        app.logger.error(err)
    
    
    return resp


if __name__ == "__main__":
    # TODO: document both PORT and IS_DEBUG env vars
    port_str = os.environ.get('PORT', '8003')
    is_debug = os.environ.get('IS_DEBUG', 'true') == 'true'
    port = int(port_str)
    app.run(host='localhost', port=port, debug=is_debug)
