from flask import Flask,url_for,request,make_response
import emr
app = Flask(__name__)

@app.get("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.post("/job")
def schedule_job():
    emr_response=emr.schedule(
                                request.json.get("name",None),
                                request.json.get("algorithm",0),
                                request.json.get("entrypoint_arguments",None)
                             )
    resp=make_response({"id":emr_response["id"]},201)
    resp.headers['Location'] = url_for('get_job', job_id=emr_response["id"])
    resp.headers['Content-Type'] = "application/json; charset=utf-8"
 
    return resp

@app.get("/job/<job_id>")
def get_job(job_id):
    return emr.get(job_id)