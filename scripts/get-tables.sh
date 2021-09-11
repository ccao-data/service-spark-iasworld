#!/bin/bash

# Check if table env var exists from docker, if not, use all tables
if [[ -z "$IPTS_TABLE" ]]; then
    JOB_TABLES=$(awk -F"," 'NR>1 {print $1}' /tmp/tables/tables-list.csv)
else
    JOB_TABLES="$IPTS_TABLE"
fi

# Create database in HDFS to store tables
DB_NAME=iasworld
hive -e "DROP DATABASE IF EXISTS ${DB_NAME}; CREATE DATABASE ${DB_NAME};"

JOB_TABLES_C=$(echo "$JOB_TABLES" | paste -sd,)
echo "Creating jobs for table(s): ${JOB_TABLES_C}"
for TABLE in ${JOB_TABLES}; do

    # Get lowercase table name
    TABLE_LC=$(echo "$TABLE" | awk '{print tolower($0)}')
    mkdir -p /tmp/bindir/"$TABLE"

    # Options passed to sqoop. Fetch 1 row of each table in order
    # to grab metadata
    SQOOP_OPTIONS_MAIN=(
        job -D oracle.sessionTimeZone=America/Chicago \
        --create "$TABLE" -- import \
        --bindir /tmp/bindir/ \
        --connect jdbc:oracle:thin:@//"$IPTS_HOSTNAME":"$IPTS_PORT"/"$IPTS_SERVICE_NAME" \
        --username "$IPTS_USERNAME" \
        --password-file file:///run/secrets/IPTS_PASSWORD \
        --query "SELECT t.*, SYS_EXTRACT_UTC(CURRENT_TIMESTAMP) AS loaded_at FROM IASWORLD.${TABLE} t WHERE \$CONDITIONS FETCH FIRST 1 ROWS ONLY" \
        --hcatalog-database "$DB_NAME" \
        --hcatalog-table "$TABLE" \
        --drop-and-create-hcatalog-table \
        --num-mappers 1
    )

    echo "Running job for table: ${TABLE}"
    hive -e "CREATE TABLE ${DB_NAME}.${TABLE_LC}(jur string)"
    sqoop "${SQOOP_OPTIONS_MAIN[@]}"

    # Execute saved sqoop job
    sqoop job --exec "$TABLE"

    # Export table definition to mounted dir
    rm -rf /tmp/hadoop-root/nm-local-dir/usercache/*
    hive -e "SHOW CREATE TABLE iasworld.${TABLE_LC}" > /tmp/tables/"$TABLE".sql
    echo "Completed job for table: ${TABLE}"
done
