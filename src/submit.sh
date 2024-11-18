#!/bin/bash

# Load environmental variables from Compose secrets file
# shellcheck disable=SC1090
set -o allexport && \
    source "$SPARK_ENV_FILE" && \
    source "$IPTS_CONNECTION_FILE" && \
    set +o allexport

# The config option used here is a runtime option that sets the port for the
# Spark application (group of jobs). Note that it must be passed as an integer,
# hence the double parenthesis. Note also that setting this statically means
# we can only run one Spark application at a time (per environment)
spark-submit \
    --conf "spark.master=${SPARK_MASTER_URL}" \
    --conf "spark.ui.port=$((SPARK_UI_PORT))" \
    --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" \
    /tmp/src/submit_jobs.py "$@"
