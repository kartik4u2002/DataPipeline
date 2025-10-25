#!/bin/bash
set -e

echo "🕒 Waiting for Flink JobManager at jobmanager:8081 ..."

# Poll the JobManager REST API until it's up (2xx)
until curl -sSf http://jobmanager:8081/overview >/dev/null 2>&1; do
  echo "Waiting for JobManager..."
  sleep 2
done

echo "✅ JobManager is ready — submitting sjob"
# Submit the Python job to the cluster JobManager (use service name 'jobmanager')
flink run -m jobmanager:8081 -py /opt/flink/flink_app/flink_job.py
