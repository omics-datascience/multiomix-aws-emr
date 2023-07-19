FROM python:3.11-alpine

ENV EMR_VIRTUAL_CLUSTER_ID "virtual-cluster-id"
ENV EMR_RELEASE_LABEL "emr-6.9.0-latest"
ENV EMR_EXECUTION_ROLE_ARN "arn-role"
ENV EMR_S3_BUCKET "bucket-name"
ENV EMR_IMAGE "283102468175.dkr.ecr.us-east-1.amazonaws.com/omicsdatascience-emr-custom:dependencies-3.8"
ENV EMR_DRIVER_TEMPLATE "driver-template-name"
ENV EMR_DRIVER_CORES 1
ENV EMR_DRIVER_MEMORY 4g
ENV EMR_EXECUTOR_TEMPLATE "executor-template-name"
ENV EMR_EXECUTOR_CORES 1
ENV EMR_EXECUTOR_MEMORY 4g
ENV EMR_DRIVER_MAX_RESULT_SIZE 2g
ENV EMR_EXECUTOR_HEARTBEAT_INTERVAL 600s
ENV EMR_NETWORK_TIMEOUT 2400s
ENV EMR_EXECUTOR_INSTANCES 3
ENV DATASETS_PATH "/var/data"
ENV RESULTS_PATH "/var/results"
ENV MULTIOMIX_URL "http://multiomix/feature-selection/aws-notification/"
ENV EKS_EMR_SERVICE_URL "multiomix-aws-emr"
ENV AWS_DEFAULT_REGION ""
ENV AWS_ACCESS_KEY_ID ""
ENV AWS_SECRET_ACCESS_KEY ""
ENV ALGO_BBHA_ENTRYPOINT "main.py"
ENV ALGO_BBHA_PY_FILES "scripts.zip"
ENV ALGO_BLIND_SEARCH_ENTRYPOINT "main.py"
ENV ALGO_BLIND_SEARCH_PY_FILES "scripts.zip"
ENV ENTRYPOINT_ARGS_KEY_PREFIX "--"

# The number of gunicorn's worker processes for handling requests.
ENV WEB_CONCURRENCY 1

# Installs Python requirements and app files
ADD config/requirements.txt /requirements.txt
RUN pip install --upgrade pip && pip install -r /requirements.txt
RUN rm -f /requirements.txt

# Adds app files
ADD src/ /app/

# Needed to make docker-compose `command` work
WORKDIR /app

EXPOSE 8000

# Runs Gunicorn
# NOTE: timeout is set to more than 2 minutes which is the timeout that other services have
ENTRYPOINT ["gunicorn", "--bind", "0.0.0.0:8000", "app:app", "--timeout", "30"]
