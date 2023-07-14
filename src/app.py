import os
import emr
import requests
import validations
from time import sleep
from multiprocessing import Process
from logging.config import dictConfig
from typing import Dict, Any, Optional
from flask import Flask, url_for, request, make_response, abort

# BioAPI version
VERSION = '0.1.14'

# Logging config
dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})

# Temporal patch to prevent issues with the EMR API sending old states to Multiomix
SLEEP_TIME: int = int(os.environ.get('SLEEP_TIME', 3))

app = Flask(__name__)


@app.get("/")
def index():
    return f"<h1>Multiomix AWS EMR integration v{VERSION}</h1>"


@app.post("/job")
def schedule_job():
    request_data = request.get_json()
    if not validations.schedule_request_is_valid(request_data):
        app.logger.warning('Invalid data received:')
        app.logger.warning(request_data)
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


def __get_job(job_id: str) -> Optional[Dict[str, Any]]:
    emr_response = emr.get(job_id)
    if emr_response is None:
        return None

    # To prevent PEP8 warning
    finished = emr_response["jobRun"]["finishedAt"].isoformat(' ') if 'finishedAt' in emr_response['jobRun'] else None

    return {
        "id": emr_response["jobRun"]["id"],
        "createdAt": emr_response["jobRun"]["createdAt"].isoformat(' '),
        "finishedAt": finished,
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


def __wait_and_send_status(job_id: str):
    """Waits some time and send request to Multiomix to change a job status."""
    # Temporal sleep to prevent retrieving an old state as EMR takes some time to update the state and inform
    # this service very quickly
    sleep(SLEEP_TIME)

    body = __get_job(job_id)

    # Gets the endpoint from env var
    multiomix_endpoint = os.getenv("MULTIOMIX_URL", '')
    multiomix_endpoint = multiomix_endpoint.rstrip('/')
    multiomix_url = f"{multiomix_endpoint}/{job_id}/"

    # Sends the request to Multiomix
    timeout = 100  # Almost 2 minutes (there's a timeout of 2 minutes in NGINX proxy)
    try:
        response = requests.post(multiomix_url, json=body, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.Timeout as err:
        app.logger.error(f'Timeout error for "{multiomix_url}":')
        app.logger.error(err)
    except requests.exceptions.ConnectionError as err:
        app.logger.error(f'Connection error for "{multiomix_url}":')
        app.logger.error(err)
    except requests.exceptions.HTTPError as err:
        app.logger.error(f'General error for "{multiomix_url}":')
        app.logger.error(err)


@app.patch("/job/<job_id>")
def change_status_job(job_id: str):
    resp = make_response({"id": job_id}, 204)
    resp.headers['Location'] = url_for('get_job', job_id=job_id)
    resp.headers['Content-Type'] = "application/json; charset=utf-8"

    # Logs some data
    state = request.json.get("state", None)
    app.logger.info(f"Job id: '{job_id}' is now in '{state}' state")

    # Makes a Thread to send the request and return the response asynchronously. This allows the EMR Spark driver
    # to terminate, setting the job in COMPLETED/FAILED status. Otherwise, the EMR job will be in RUNNING state
    # and Multiomix would receive that invalid state.
    Process(target=__wait_and_send_status, args=(job_id,), daemon=True).start()

    return resp


if __name__ == "__main__":
    # TODO: document both PORT and IS_DEBUG env vars
    port_str = os.environ.get('PORT', '8003')
    is_debug = os.environ.get('DEBUG', 'true') == 'true'
    port = int(port_str)
    app.run(host='127.0.0.1', port=port, debug=is_debug)
