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


## Deploying


### Amazon AWS

Soon...
<!-- Complete by Julian -->


### Local

You can run in local to use from your local [Multiomix deploy][multiomix-deploying]

1. Make a copy of `docker-compose_dist.yml` as `docker-compose.yml`
1. Modify any parameter you need.
1. Run `docker-compose up -d`


## License

This code is distributed under the MIT license.

[multiomix-deploying]: https://github.com/omics-datascience/multiomix/blob/main/DEPLOYING.md
