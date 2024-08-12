#!/bin/bash

# Example submission of jobs from the test_jobs key in the default file
yq -o=json .test_jobs /tmp/config/default_jobs.yaml \
    | spark-submit submit_jobs.py --json-string "$(cat -)"
