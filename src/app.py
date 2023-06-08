import os
import emr
from flask import Flask, url_for, request, make_response

app = Flask(__name__)


@app.get("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.post("/job")
def schedule_job():
    emr_response = emr.schedule(
        request.json.get("name", None),  # TODO: make mandatory
        request.json.get("algorithm", 0),  # TODO: make mandatory
        request.json.get("entrypoint_arguments", None)  # TODO: make mandatory
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
    return emr.get(job_id)


@app.delete("/job/<job_id>")
def cancel_job(job_id):
    return emr.cancel(job_id)


@app.patch("/job/<job_id>")
def change_status_job(job_id):
    resp = make_response({"id": job_id}, 204)
    resp.headers['Location'] = url_for('get_job', job_id=job_id)
    resp.headers['Content-Type'] = "application/json; charset=utf-8"
    app.logger.info("Job id: '{id}' is now in '{state}' state".format(id=job_id, state=request.json.get("state", None)))
    # TODO: implement logic to notify Upwards to the MultiomixURL/feature-selection/aws-notification/<job_id>/ endpoint
    # TODO: check parameters to send with Camele

    return resp


if __name__ == "__main__":
    # TODO: document both PORT and IS_DEBUG env vars
    port_str = os.environ.get('PORT', '8003')
    is_debug = os.environ.get('IS_DEBUG', 'true') == 'true'
    port = int(port_str)
    app.run(host='localhost', port=port, debug=is_debug)
