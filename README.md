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

## Virtual Environment Variables

To have a working application that interacts with EMR correctly we need to set the following Environment Variables.

| Variable | Default Value                                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|---|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| EMR_VIRTUAL_CLUSTER_ID |                                                        | The EMR virtual cluster id that the app will be talking with. Remember that the user set must have a valid role for the correct interaction with EMR                                                                                                                                                                                                                                                                                                                                    |
| EMR_RELEASE_LABEL | `emr-6.9.0-latest`                                     | The version of release that will be used for the EMR Jobs. It match with the base image used in the pod template.                                                                                                                                                                                                                                                                                                                                                                       |
| EMR_EXECUTION_ROLE_ARN |                                                        | The arn that will be used by the scheduled job.                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| EMR_S3_BUCKET |                                                        | The S3 bucket that contains all the the files required for the job. These files are `entrypoint scripts`, `pod templates`, `py-files zips` and `logs` target directory.                                                                                                                                                                                                                                                                                                                 |
| EMR_IMAGE |                                                        | The full URI image that job containers are going to run.                                                                                                                                                                                                                                                                                                                                                                                                                                |
| EMR_DRIVER_TEMPLATE |                                                        | The kubernetes pod object template that the driver pod will use.                                                                                                                                                                                                                                                                                                                                                                                                                        |
| EMR_DRIVER_CORES | `1`                                                    | The driver wanted cores.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| EMR_DRIVER_MEMORY | `4g`                                                   | `spark.driver.memory` Spark parameter. It's the driver memory limit.                                                                                                                                                                                                                                                                                                                                                                                                                   |
| EMR_EXECUTOR_TEMPLATE |                                                        | The kubernetes pod object template that the executor pod will use.                                                                                                                                                                                                                                                                                                                                                                                                                      |
| EMR_EXECUTOR_CORES | `1`                                                    | The executor wanted cores.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| EMR_EXECUTOR_MEMORY | `4g`                                                   | `spark.executor.memory` Spark parameter. It's the executor memory limit.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| EMR_DRIVER_MAX_RESULT_SIZE | `2g`                                                   | `spark.driver.maxResultSize` Spark parameter. It's the limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on `EMR_DRIVER_MEMORY` and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors. |
| EMR_EXECUTOR_HEARTBEAT_INTERVAL | `600s`                                                 | `spark.executor.heartbeatInterval` Spark parameter. It's the interval between each executor's heartbeats to the driver. Heartbeats let the driver know that the executor is still alive and update it with metrics for in-progress tasks. This parameter should be significantly less than `EMR_EXECUTOR_HEARTBEAT_INTERVAL`.                                                                                                                                                           |
| EMR_NETWORK_TIMEOUT | `2400s`                                                | `spark.network.timeout` Spark parameter. It's the default timeout for all network interactions.                                                                                                                                                                                                                                                                                                                                                                                         |
| EMR_EXECUTOR_INSTANCES | `3`                                                    | The amount of executor instances that EMR is going to spin up.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| DATASETS_PATH | `/var/data`                                            | The path in where the python scripts are going to find the processed datasets. Keep in mind that this should match the pod templates and script needs.                                                                                                                                                                                                                                                                                                                                  |
| RESULTS_PATH | `/var/results`                                         | The path in where the python scripts are going to place the execution results. Keep in mind that this should match the pod templates and script needs.                                                                                                                                                                                                                                                                                                                                  |
| MULTIOMIX_URL | `http://multiomix/feature-selection/aws-notification/` | The url to reach multiomix instance. It will be used to report the job status upwards to multiomix.                                                                                                                                                                                                                                                                                                                                                                                     |
| EKS_EMR_SERVICE_URL | `multiomix-aws-emr`                                    | Our url to let know EMR how to reach back this microservice. When the job ends it could make a request to let the microservice that jobs end. This callback will depend on the pod templates.                                                                                                                                                                                                                                                                                           |
| AWS_DEFAULT_REGION | `us-east-1`                                            | The main aws region that will be used for locate EMR cluster.                                                                                                                                                                                                                                                                                                                                                                                                                           |
| AWS_ACCESS_KEY_ID |                                                        | The aws user's access key that will be used by boto3.                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| AWS_SECRET_ACCESS_KEY |                                                        | The aws user's secret access key that will be used by boto3.                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ALGO_BLIND_SEARCH_ENTRYPOINT | `main.py`                                              | The Blind Search algorithm script's name that will be told to EMR to use for this job.                                                                                                                                                                                                                                                                                                                                                                                                  |
| ALGO_BLIND_SEARCH_PY_FILES | `scripts.zip`                                          | The Blind Search algorithm py-files required for the main script that will be told to EMR to use for this job.                                                                                                                                                                                                                                                                                                                                                                          |
| ALGO_BBHA_ENTRYPOINT | `main.py`                                              | The BBHA algorithm script's name that will be told to EMR to use for this job.                                                                                                                                                                                                                                                                                                                                                                                                          |
| ALGO_BBHA_PY_FILES | `scripts.zip`                                          | The BBHA algorithm py-files required for the main script that will be told to EMR to use for this job.                                                                                                                                                                                                                                                                                                                                                                                  |
| ENTRYPOINT_ARGS_KEY_PREFIX | `--`                                                   | The prefix for the entrypoint arguments that the main script will accept.                                                                                                                                                                                                                                                                                                                                                                                                               |



## Deploying


### Amazon AWS

1. Create a policy with the following rights:
    ```
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "sid0",
                "Effect": "Allow",
                "Action": [
                    "emr-containers:CancelJobRun",
                    "emr-containers:DescribeJobRun"
                ],
                "Resource": "arn:aws:emr-containers:*:{account}:/virtualclusters/*/jobruns/*"
            },
            {
                "Sid": "sid1",
                "Effect": "Allow",
                "Action": [
                    "emr-containers:DescribeVirtualCluster",
                    "emr-containers:ListJobRuns",
                    "emr-containers:StartJobRun"
                ],
                "Resource": "arn:aws:emr-containers:*:{account}:/virtualclusters/*"
            },
            {
                "Sid": "sid2",
                "Effect": "Allow",
                "Action": "emr-containers:ListVirtualClusters",
                "Resource": "*"
            }
        ]
    }
    ```
1. Assing the policy to one User and get the Access Key and Secret Access key that will be used in the environment variables.
1. Deploy the image in the service you want use with the ENV Vars correctly filled. There's an example of deployment template for Kubernetes called `deployment.yaml` in the `templates/kubernetes` folder.

### Local

You can run in local to use from your local [Multiomix deploy][multiomix-deploying]

1. Make a copy of `docker-compose_dist.yml` as `docker-compose.yml`
1. Modify any parameter you need.
1. Run `docker-compose up -d`


## License

This code is distributed under the MIT license.

[multiomix-deploying]: https://github.com/omics-datascience/multiomix/blob/main/DEPLOYING.md
