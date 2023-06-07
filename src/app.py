import os
import emr
from flask import Flask, url_for, request, make_response,abort

app = Flask(__name__)


@app.get("/")
def hello_world():
    return "<p>Hello, World!</p>"


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
    resp = make_response({"id": emr_response["id"]}, 200)
    resp.headers['Location'] = url_for('get_job', job_id=emr_response["id"])
    resp.headers['Content-Type'] = "application/json; charset=utf-8"

    return resp


@app.patch("/job/<job_id>")
def change_status_job(job_id):
    resp = make_response({"id": job_id}, 204)
    resp.headers['Location'] = url_for('get_job', job_id=job_id)
    resp.headers['Content-Type'] = "application/json; charset=utf-8"
    app.logger.info("Job id: '{id}' is now in '{state}' state".format(id=job_id, state=request.json.get("state", None)))
    # Logic to notify Upwards
    return resp


if __name__ == "__main__":
    port_str = os.environ.get('PORT', '8003')
    is_debug = os.environ.get('IS_DEBUG', 'true') == 'true'
    port = int(port_str)
    app.run(host='localhost', port=port, debug=is_debug)
