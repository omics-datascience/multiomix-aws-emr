#!/bin/bash

WORKDIR=${INIT_WORKDIR:-"/root"}
S3_BUCKET=${EMR_S3_BUCKET:-""}

echo "Getting manifest ..."
JOB_ID=$(cut -d "-" -f2 <<< $HOSTNAME)
aws s3 cp "s3://$S3_BUCKET/manifests/$JOB_ID.manifest" /tmp/$JOB_ID.manifest

DATASET=$(cat /tmp/$JOB_ID.manifest 2>/dev/null | jq .dataset -r)
if [ "$DATASET" = "false" ]
then
    echo "No dataset will be used."
    echo "Starting job..."
    exit 0
elif [ "$DATASET" = "" ]
then
    echo "Dataset value it's empty."
    echo "If the job doesn't use dataset, you should use 'false' as dataset value in manifest."
    exit 1
fi
echo "Getting dataset $DATASET ..."
aws s3 cp "s3://$S3_BUCKET/datasets/$DATASET" /tmp/$DATASET

echo "Extracting dataset $DATASET in /var/data/ ..."
tar xvfz /tmp/$DATASET -C /var/data/

DATASET=$(echo $DATASET | cut -d "." -f1)
FILE_COUNT=$(ls -l /var/data/$DATASET 2>/dev/null| wc -l)

if [ $FILE_COUNT -lt 1 ]
then
    echo "No files were extracted in /var/data/$DATASET"
    exit 1
fi

echo "Starting job..."