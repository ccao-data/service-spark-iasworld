#!/bin/bash

# Example command to execute test jobs in the Spark cluster.
# First run `docker compose up -d` in the repo root
docker exec spark-node-master-prod ./submit.sh \
    --yaml-file default_jobs/weekday_jobs.yaml
