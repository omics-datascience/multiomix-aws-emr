# multiomix-aws-emr

AWS EMR Spark Job abstraction for Multiomix project.


## Pre-requisites

- Python >= 3.7 (tested with 3.10 version)


## Installation 

1. Create a Python virtual environment to install some dependencies:
    1. `cd src`
    1. `python3 -m venv venv`
    1. `source venv/bin/activate` (run only when you need to work)
    1. `pip install -r ./src/config/requirements.txt`. Maybe you need to run `python3.exe -m pip install -r ./src/config/requirements.txt` in Windows instead.


## Running server for development

1. Activate your virtual environment: `source venv/bin/activate`
1. Go to `src` directory and run: `python3 app.py`


## License

This code is distributed under the MIT license.
