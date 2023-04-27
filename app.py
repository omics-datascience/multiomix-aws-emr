from flask import Flask,url_for,request
import scheduler
app = Flask(__name__)

@app.get("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.post("/schedule")
def schedule_job():
    
    return scheduler.schedule(
                                request.json.get("name",None),
                                request.json.get("algorithm",0),
                                request.json.get("dataset","Breast_Invasive_Carcinoma.tar.gz")
                             )