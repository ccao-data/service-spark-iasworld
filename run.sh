#!/bin/bash

# Example command to execute test jobs in the Spark cluster
# First run `docker compose up -d` in the repo root
docker compose exec spark-node-master-prod ./submit.sh \
    --json-string "$(yq -o=json .test_jobs ./config/default_jobs.yaml)"
