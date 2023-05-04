import random
import string
import os
import boto3
from enum import Enum



class Algoritms(Enum):
    BLIND_SEARCH = 0
    BBHA = 1

def schedule(
                name,
                algorithm,
                dataset,
                entrypoint_arguments=None
            ):
    args=_get_args(name,algorithm, dataset,entrypoint_arguments)
    client = boto3.client('emr-containers')
    response=None
    try:
        response = client.start_job_run(
            name=args['name'],
            virtualClusterId=args['virtual_cluster'],
            executionRoleArn=args['execution_role'],
            releaseLabel=args['release_label'],
            jobDriver={
                "sparkSubmitJobDriver": {
                    "entryPoint": "s3://{bucket}/scripts/{entrypoint}".format(bucket=args['bucket'],entrypoint=args['entrypoint']),
                    "entryPointArguments":args['entrypoint_arguments'],
                    "sparkSubmitParameters":get_spark_submit_params_str(args)
                }
            },
            configurationOverrides={
                "monitoringConfiguration":{
                    "s3MonitoringConfiguration":{
                        "logUri": "s3://{bucket}/joblogs".format(bucket=args['bucket'])
                    }
                }
            }
        )
    except client.exceptions.ValidationException as err:
        print("Job validation exception")
        print(err.response['Error']['Message'])
    except client.exceptions.ResourceNotFoundException as err:
        print("Resource not found exception")
        print(err.response['Error']['Message'])
    except client.exceptions.InternalServerException as err:
        print(err.response['Error']['Message'])

    return response

def get_spark_submit_params_str(args):
    spark_submit_params="--py-files s3://{bucket}/py-files/{py_files} --conf spark.kubernetes.driver.podTemplateFile=s3://{bucket}/templates/{driver_template} --conf spark.kubernetes.executor.podTemplateFile=s3://{bucket}/templates/{executor_template} " + \
        "--conf spark.kubernetes.container.image={image} --conf spark.executor.cores={executor_cores} --conf spark.executor.memory={executor_memory} --conf spark.driver.cores={driver_cores} " + \
        "--conf spark.driver.memory={driver_memory} --conf spark.executor.instances={executor_instances} --conf spark.kubernetes.driverEnv.DATASET_NAME={dataset} " + \
        "--conf spark.kubernetes.driverEnv.DATASETS_PATH={datasets_path} --conf spark.kubernetes.driverEnv.RESULTS_PATH={results_path} --conf spark.kubernetes.driverEnv.JOB_NAME={name}"
    return spark_submit_params.format(
            bucket=args['bucket'],
            dataset=args['dataset'].replace(".tar.gz", ""),
            py_files=args['py_files'],
            driver_template=args['driver_template'],
            executor_template=args['executor_template'],
            image=args['image'],
            executor_cores=args['executor_cores'],
            executor_memory=args['executor_memory'],
            driver_cores=args['driver_cores'],
            driver_memory=args['driver_memory'],
            executor_instances=args['executor_instances'],
            datasets_path=args['datasets_path'],
            results_path=args['results_path'],
            name=args['name']
    )

def _get_args(
                name,
                algorithm,
                dataset,
                entrypoint_args=None
             ):
    if name is None:
        name=_get_random_name(Algoritms(algorithm).name)
    if entrypoint_args is None:
        entrypoint_args=[]       
    return {
        "name": name,
        "entrypoint": os.getenv('ALGO_'+Algoritms(algorithm).name+'_ENTRYPOINT'),
        "entrypoint_arguments": entrypoint_args,
        "py_files": os.getenv('ALGO_'+Algoritms(algorithm).name+'_PY_FILES'),
        "bucket": os.getenv('EMR_S3_BUCKET'),
        "driver_template": os.getenv('EMR_DRIVER_TEMPLATE'),
        "driver_cores": os.getenv('EMR_DRIVER_CORES'),
        "driver_memory": os.getenv('EMR_DRIVER_MEMORY'),        
        "executor_template": os.getenv('EMR_EXECUTOR_TEMPLATE'),
        "executor_cores": os.getenv('EMR_EXECUTOR_CORES'),
        "executor_memory": os.getenv('EMR_EXECUTOR_MEMORY'),
        "executor_instances": os.getenv('EMR_EXECUTOR_INSTANCES'),
        "execution_role": os.getenv('EMR_EXECUTION_ROLE_ARN'),
        "image": os.getenv('EMR_IMAGE'),
        "virtual_cluster": os.getenv('EMR_VIRTUAL_CLUSTER_ID'),
        "release_label": os.getenv('EMR_RELEASE_LABEL'),
        "dataset":dataset,
        "datasets_path": os.getenv('DATASETS_PATH'),
        "results_path": os.getenv('RESULTS_PATH'),
        "return_url": os.getenv('EKS_EMR_SERVICE_URL'), # EMR will inform to this url that the job ends. 
                                                        # This Service then, will inform multiomix to MULTIOMIX_URL
    }

def _get_random_name(algorithm):
    return 'multiomix-'+algorithm.lower().replace('_','-')+'-'+''.join(random.choices(string.ascii_lowercase, k=6))+'-'+''.join(random.choices(string.digits, k=6))

def get_algoritm_details(algoritm):
    entrypoint=None
    py_files=None
    if algoritm == Algoritms.BBHA:
        pass

def preflight_checks(args):
    fail=False
    try:
        args.name.index(' ')
        print("- JOB_NAME cannot contain spaces.")
        fail=True
    except:
        fail=False

    if args.virtual_cluster is None:
        print("- EMR_VIRTUAL_CLUSTER_ID environment variable is not set and no valid argument was given.")
        fail=True

    if args.release_label is None:
        print("- EMR_RELEASE_LABEL environment variable is not set and no valid argument was given.")
        fail=True

    if args.execution_role is None:
        print("- EMR_EXECUTION_ROLE_ARN environment variable is not set and no valid argument was given.")
        fail=True
    
    if args.bucket is None:
        print("- EMR_S3_BUCKET environment variable is not set and no valid argument was given.")
        fail=True

    if fail:
        exit(1)