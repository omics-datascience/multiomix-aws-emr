#!/bin/bash
export EMR_VIRTUAL_CLUSTER_ID="virtual-cluster-id"
export EMR_RELEASE_LABEL="emr-6.9.0-latest"
export EMR_EXECUTION_ROLE_ARN="arn-role"
export EMR_S3_BUCKET="bucket-name"
export EMR_IMAGE="283102468175.dkr.ecr.us-east-1.amazonaws.com/omicsdatascience-emr-custom:dependencies-3.8"
export EMR_DRIVER_TEMPLATE="driver-template-name"
export EMR_DRIVER_CORES=1
export EMR_DRIVER_MEMORY=2g
export EMR_EXECUTOR_TEMPLATE="executor-template-name"
export EMR_EXECUTOR_CORES=1
export EMR_EXECUTOR_MEMORY=2g
export EMR_EXECUTOR_INSTANCES=3
export DATASETS_PATH="/var/data"
export RESULTS_PATH="/var/results"
export MULTIOMIX_URL="http://multiomix/feature-selection/aws-notification/"
export EKS_EMR_SERVICE_URL="multiomix-aws-emr"
export AWS_DEFAULT_REGION=""
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export ALGO_BLIND_SEARCH_ENTRYPOINT="main.py"
export ALGO_BLIND_SEARCH_PY_FILES="scripts.zip"
export ALGO_BBHA_ENTRYPOINT="main.py"
export ALGO_BBHA_PY_FILES="scripts.zip"
export ENTRYPOINT_ARGS_KEY_PREFIX="--"