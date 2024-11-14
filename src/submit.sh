#!/bin/bash

# The config option used here is a runtime option that sets the port for the
# Spark application (group of jobs). Note that it must be passed as an integer,
# hence the double parenthesis. Note also that setting this statically means
# we can only run one Spark application at a time (per environment)
spark-submit --conf "spark.ui.port=$((SPARK_UI_PORT))" /tmp/src/submit_jobs.py "$@"
