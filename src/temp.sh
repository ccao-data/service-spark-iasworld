#!/bin/bash
yq -o=json .test_jobs /tmp/src/default_jobs.yaml >/tmp/test_jobs.json
spark-submit submit.py --json-file /tmp/test_jobs.json
