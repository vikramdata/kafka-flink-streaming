#!/usr/bin/env bash
# Submits the Flink SQL job once the cluster is up. Safe to re-run: if the job is
# already running it does nothing, so `docker compose up` never creates duplicates.
set -euo pipefail

JOB_NAME="realtime-order-analytics"

echo "Waiting for Flink JobManager..."
until (echo > /dev/tcp/jobmanager/8081) 2>/dev/null; do sleep 2; done

if /opt/flink/bin/flink list -r -m jobmanager:8081 2>/dev/null | grep -q "$JOB_NAME"; then
  echo "Job '$JOB_NAME' is already running - nothing to do."
  exit 0
fi

{
  echo "SET 'pipeline.name' = '$JOB_NAME';"
  cat /opt/sql/01_source.sql /opt/sql/02_sinks.sql /opt/sql/03_pipelines.sql
} > /tmp/job.sql

echo "Submitting '$JOB_NAME'..."
/opt/flink/bin/sql-client.sh -f /tmp/job.sql
echo "Submitted. Open http://localhost:8081 to watch it run."
