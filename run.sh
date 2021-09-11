#!/bin/bash
START_TIME=$(date +%s)

# Env variables controlling where sqooped data is exported to
TEMP_LOG_FILE="logs/temp-sqoop-log"
BACKUP_LOG_FILE="logs/backup-sqoop-log"
BUCKET_URI="s3://ccao-data-warehouse-us-east-1"
CRAWLER_NAME="ccao-data-warehouse-iasworld-crawler"
LOG_GROUP_NAME="/ccao/jobs/sqoop"

# Run all sqoop jobs to extract tables
/usr/local/bin/docker-compose up \
    --abort-on-container-exit \
    --remove-orphans \
    --no-color \
    | ts '%.s' \
    | tee "$TEMP_LOG_FILE"

# Cleanup after docker run
/usr/local/bin/docker-compose rm -f -s -v

# Drop existing keys on S3 for any pulled tables. Will only drop folders for
# which there are local taxyr replacements
find target/ -maxdepth 1 -type d -empty -delete
TABLES_EXTRACTED=$(ls target/)
for TABLE in ${TABLES_EXTRACTED}; do
    SUB_DIRS=$(find target/"$TABLE"/* -maxdepth 1 -type d -exec basename {} \;)
    if [[ -n "$SUB_DIRS" && $(echo "$SUB_DIRS" | wc -l) -gt 0 ]]; then
        for dir in ${SUB_DIRS}; do
            /usr/bin/aws s3 rm \
                "$BUCKET_URI"/iasworld/"$TABLE"/"$dir" \
                --exclude "*" \
                --include "*.parquet" \
                --recursive \
                | ts '%.s' \
                | tee -a "$TEMP_LOG_FILE"
        done
    else
        /usr/bin/aws s3 rm \
            "$BUCKET_URI"/iasworld/"$TABLE" \
            --exclude "*" \
            --include "*.parquet" \
            --recursive \
            | ts '%.s' \
            | tee -a "$TEMP_LOG_FILE"
    fi
done

# Uploaded pulled files from local target/ dir to S3
/usr/bin/aws s3 mv \
    target/ \
    "$BUCKET_URI"/iasworld \
    --exclude "*" \
    --include "*.parquet" \
    --recursive \
    --no-progress \
    | ts '%.s' \
    | tee -a "$TEMP_LOG_FILE"

# Delete any remaining empty dirs
find target/ -type d -empty -delete

# Kick off Glue crawler run. Not strictly necessary since 99%
# of the time we're not creating new partitions or columns,
# but still nice to run
echo "Starting AWS Glue crawler run" | ts '%.s' | tee -a "$TEMP_LOG_FILE"
/usr/bin/aws glue start-crawler --name "$CRAWLER_NAME" || true

# Trigger a workflow to run all dbt tests now that new data is uploaded, but
# don't let this step crash the log upload
(source scripts/dispatch-dbt-workflow.sh || true) \
    | ts '%.s' \
    | tee -a "$TEMP_LOG_FILE"

# Print overall runtime stats and tables extracted
END_TIME=$(date +%s)
RUNTIME=$((END_TIME - START_TIME))
HH=$((RUNTIME / 3600))
MM=$(((RUNTIME % 3600) / 60))
SS=$(((RUNTIME % 3600) % 60))
echo "Tables extracted: ${TABLES_EXTRACTED}" \
    | ts '%.s' \
    | tee -a "$TEMP_LOG_FILE"
echo "Total extraction time: ${HH}:${MM}:${SS} (hh:mm:ss)" \
    | ts '%.s' \
    | tee -a "$TEMP_LOG_FILE"

# Bash function to convert text log to JSON consumable by CloudWatch
parse_logs() {
    sed 's/ /|/' "$1" \
        | sed 's/\([0-9]\.[0-9]\{3\}\)[0-9]\{1,\}/\1/' \
        | sed 's/\.//' \
        | awk '{print $1, $NF}' OFS='|' FS='|' \
        | jq -Rn '[inputs
        | . / "\n"
        | (.[] | select(length > 0) | . / "|") as $input
        | {"timestamp": $input[0]|tonumber, "message": $input[1]}]' \
            > "$2"
}

# Create log stream in CloudWatch with today's date
LOG_STREAM_NAME="sqoop-job-log-$(date -u +'%Y-%m-%d_%H-%M-%S')"
/usr/bin/aws logs create-log-stream \
    --log-group-name "$LOG_GROUP_NAME" \
    --log-stream-name "$LOG_STREAM_NAME"

# Convert text output from Docker and AWS CLI to clean JSON
# for upload to AWS CloudWatch. Logs need to be split into chunks for upload
# First loop skips using sequence token necessary for subsequent uploads
COUNTER=1
split -l 4000 "$TEMP_LOG_FILE" logs/temp-sqoop-log-
for f in logs/temp-sqoop-log-*; do
    parse_logs "$f" "$f".json
    if [[ "$COUNTER" -eq 1 ]]; then
        SEQ_TOKEN=$(
            /usr/bin/aws logs put-log-events \
                --log-group-name "$LOG_GROUP_NAME" \
                --log-stream-name "$LOG_STREAM_NAME" \
                --log-events file://"$f".json \
                | jq -r .nextSequenceToken
        )
    else
        SEQ_TOKEN=$(
            /usr/bin/aws logs put-log-events \
                --log-group-name "$LOG_GROUP_NAME" \
                --log-stream-name "$LOG_STREAM_NAME" \
                --log-events file://"$f".json \
                --sequence-token "$SEQ_TOKEN" \
                | jq -r .nextSequenceToken
        )
    fi
    COUNTER=$((COUNTER + 1))
done
echo "Logs successfully uploaded to CloudWatch"

# Remove uploaded log files
mv ./"$TEMP_LOG_FILE" ./"$BACKUP_LOG_FILE"
if [[ -n "$TEMP_LOG_FILE" ]]; then rm ./"$TEMP_LOG_FILE"*; fi
