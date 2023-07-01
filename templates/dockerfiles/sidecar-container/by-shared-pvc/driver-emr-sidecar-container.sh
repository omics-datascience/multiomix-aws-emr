#!/bin/bash
# Base template taken from here -> https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/pod-templates.html
SERVICE_URL=${EKS_EMR_SERVICE_URL:-"multiomix-aws-emr"}
SERVICE_PROTO=${EKS_EMR_SERVICE_PROTO:-"http"}
SERVICE_PORT=${EKS_EMR_SERVICE_PORT:-80}
FLUENTD_PATH=${HEARTBEAT_PATH:-"/var/log/fluentd"}
APP_LOG_PATH=${APP_LOG_PATH:-"/emr/app"}
EVENT_LOG_PATH=${EVENT_LOG_PATH:-"/emr/event"}
RESULTS=${RESULTS_PATH:-"/var/results"}
FILE_TO_WATCH="$FLUENTD_PATH/main-container-terminated"
INITIAL_HEARTBEAT_TIMEOUT_THRESHOLD=60
HEARTBEAT_TIMEOUT_THRESHOLD=15
SLEEP_DURATION=10

function terminate_main_process() {
    JOB_ID=$(cut -d "-" -f2 <<< $HOSTNAME)
    echo "Results are in $RESULTS ..."
    echo "SparkListenerJobEnd event founded ..."
    STATE=$(grep "SparkListenerJobEnd" $EVENT_LOG_PATH/spark-$JOB_ID | jq '."Job Result".Result' -r)
    echo "Job state was $STATE ..."
    echo "Notifying service in $SERVICE_PROTO://$SERVICE_URL:$SERVICE_PORT ..."
    curl "$SERVICE_PROTO://$SERVICE_URL:$SERVICE_PORT/job/$JOB_ID" -X PATCH -H "Content-Type: application/json" -d "{\"state\":\"Terminated\"}" -vv

}

# Waiting for the first heartbeat sent by Spark main container
echo "Waiting for file $FILE_TO_WATCH to appear..."
start_wait=$(date +%s)
while ! [[ -f "$FILE_TO_WATCH" ]]; do
    elapsed_wait=$(expr $(date +%s) - $start_wait)
    if [ "$elapsed_wait" -gt "$INITIAL_HEARTBEAT_TIMEOUT_THRESHOLD" ]; then
        echo "File $FILE_TO_WATCH not found after $INITIAL_HEARTBEAT_TIMEOUT_THRESHOLD seconds; aborting"
        terminate_main_process
        exit 1
    fi
    sleep $SLEEP_DURATION;
done;
echo "Found file $FILE_TO_WATCH; watching for heartbeats..."

while [[ -f "$FILE_TO_WATCH" ]]; do
    LAST_HEARTBEAT=$(stat -c %Y $FILE_TO_WATCH)
    ELAPSED_TIME_SINCE_AFTER_HEARTBEAT=$(expr $(date +%s) - $LAST_HEARTBEAT)
    if [ "$ELAPSED_TIME_SINCE_AFTER_HEARTBEAT" -gt "$HEARTBEAT_TIMEOUT_THRESHOLD" ]; then
        echo "Last heartbeat to file $FILE_TO_WATCH was more than $HEARTBEAT_TIMEOUT_THRESHOLD seconds ago at $LAST_HEARTBEAT; terminating"
        terminate_main_process
        exit 0
    fi
    sleep $SLEEP_DURATION;
done;
echo "Outside of loop, main-container-terminated file no longer exists"
    
# The file will be deleted once the fluentd container is terminated

echo "The file $FILE_TO_WATCH doesn't exist any more;"
terminate_main_process
exit 0
