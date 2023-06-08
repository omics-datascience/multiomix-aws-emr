import os
import emr
from flask import Flask, url_for, request, make_response,abort

# BioAPI version
VERSION = '0.1.1'

app = Flask(__name__)


@app.get("/")
def index():
    return f"<h1>Multiomix AWS EMR integration v{VERSION}</h1>"


@app.post("/job")
def schedule_job():
    request_data=request.get_json()
    if request_data is None or \
            "name" not in request_data or \
            "algorithm" not in request_data or \
            "entrypoint_arguments" not in request_data:
        abort(400)
            

    emr_response = emr.schedule(
        request_data["name"],
        request_data["algorithm"],
        request_data["entrypoint_arguments"]
    )

    # If the response is None, returns a 500 error
    if emr_response is None:
        return make_response({"error": "Internal server error"}, 500)

    resp = make_response({"id": emr_response["id"]}, 201)
    resp.headers['Location'] = url_for('get_job', job_id=emr_response["id"])
    resp.headers['Content-Type'] = "application/json; charset=utf-8"

    return resp


@app.get("/job/<job_id>")
def get_job(job_id):
    emr_response=emr.get(job_id)
    if emr_response is None:
        abort(404)

    resp = make_response(
                        {
                            "id": emr_response["jobRun"]["id"],
                            "createdAt": emr_response["jobRun"]["createdAt"],
                            "finishedAt": emr_response["jobRun"]["finishedAt"],
                            "name":emr_response["jobRun"]["name"],
                            "state": emr_response["jobRun"]["state"],
                            "stateDetails": emr_response["jobRun"]["stateDetails"],                         
                        }, 200)
    resp.headers['Content-Type'] = "application/json; charset=utf-8"    
    return resp


@app.delete("/job/<job_id>")
def cancel_job(job_id):
    emr_response=emr.cancel(job_id)
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
    # TODO: implement logic to notify Upwards to the MultiomixURL/feature-selection/aws-notification/<job_id>/ endpoint
    # TODO: send the same fields as get_job() returns

    return resp


if __name__ == "__main__":
    # TODO: document both PORT and IS_DEBUG env vars
    port_str = os.environ.get('PORT', '8003')
    is_debug = os.environ.get('IS_DEBUG', 'true') == 'true'
    port = int(port_str)
    app.run(host='localhost', port=port, debug=is_debug)
