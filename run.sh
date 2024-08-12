#!/bin/bash

# Example command to execute test jobs in the Spark cluster
# First run `docker compose up` in the repo root
docker exec -it spark-node-master ./submit.sh --json-string \
    "$(yq -o=json .test_jobs ./config/default_jobs.yaml)"