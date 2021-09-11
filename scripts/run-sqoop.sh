#!/bin/bash
MAX_MAPPERS=32

# Check if table env var exists from docker, if not, use all tables
if [[ -z "$IPTS_TABLE" ]]; then
    JOB_TABLES=$(awk -F"," 'NR>1 {print $1}' /tmp/tables/tables-list.csv)
else
    JOB_TABLES=$(echo "$IPTS_TABLE" | tr -cd '[:alpha:][:space:]_')
fi

# Create database in HDFS to store tables
DB_NAME=iasworld
hive -e "DROP DATABASE IF EXISTS ${DB_NAME}; CREATE DATABASE ${DB_NAME};"

echo "Creating jobs for table(s): ${JOB_TABLES}"
for TABLE in ${JOB_TABLES}; do

    # Get lowercase table name
    TABLE_LC=$(echo "$TABLE" | awk '{print tolower($0)}')
    mkdir -p /tmp/bindir/"$TABLE"

    # Boolean value for whether $TABLE contains the TAXYR column
    CONTAINS_TAXYR=$(
        awk -F"," \
            -v table="$TABLE" \
            '$1 == table {print $2}' \
            /tmp/tables/tables-list.csv \
            | tr -d "\n" | tr -d "\r"
    )

    # Get the number of buckets to use for Hive partitioning
    NUM_BUCKETS=$(
        awk -F"," \
            -v table="$TABLE" \
            '$1 == table {print $3}' \
            /tmp/tables/tables-list.csv \
            | tr -d "\n" | tr -d "\r"
    )

    # Construct a query based on specified year and condition
    QUERY_YEAR=$(echo "$IPTS_TABLE" | grep -Po "(?<=${TABLE}.)[0-9]{4}")
    QUERY_COND=$(echo "$IPTS_TABLE" | grep -Po "(?<=${TABLE})[<>=]")
    if [[ -z "$QUERY_YEAR" && -z "$QUERY_COND" ]]; then
        NUM_MAPPERS=$(($(date +%Y) - 1998))
        NUM_MAPPERS=$((NUM_MAPPERS < MAX_MAPPERS ? NUM_MAPPERS : MAX_MAPPERS))
        BOUNDARY_QUERY="SELECT MIN(TAXYR), MAX(TAXYR) FROM IASWORLD.${TABLE}"
        QUERY="SELECT t.*, SYS_EXTRACT_UTC(CURRENT_TIMESTAMP) AS loaded_at
               FROM IASWORLD.${TABLE} t
               WHERE \$CONDITIONS"
    else
        # Make number of mappers roughly equal to number of tax years
        if [[ "$QUERY_COND" == \> ]]; then
            NUM_MAPPERS=$(($(date +%Y) - QUERY_YEAR))
        elif [[ ${QUERY_COND} == \< ]]; then
            NUM_MAPPERS=$((QUERY_YEAR - 1998))
        else
            NUM_MAPPERS=1
        fi
        NUM_MAPPERS=$((NUM_MAPPERS < MAX_MAPPERS ? NUM_MAPPERS : MAX_MAPPERS))
        BOUNDARY_QUERY="SELECT MIN(TAXYR), MAX(TAXYR) FROM IASWORLD.${TABLE}
                        WHERE TAXYR ${QUERY_COND} ${QUERY_YEAR}"
        QUERY="SELECT t.*, SYS_EXTRACT_UTC(CURRENT_TIMESTAMP) AS loaded_at
               FROM IASWORLD.${TABLE} t
               WHERE TAXYR ${QUERY_COND} ${QUERY_YEAR}
               AND \$CONDITIONS"
    fi

    # Options passed to sqoop
    SQOOP_OPTIONS_MAIN=(
        job -D oracle.sessionTimeZone=America/Chicago
        --create "$TABLE" -- import
        --bindir /tmp/bindir/"$TABLE"
        --connect jdbc:oracle:thin:@//"$IPTS_HOSTNAME":"$IPTS_PORT"/"$IPTS_SERVICE_NAME"
        --username "$IPTS_USERNAME"
        --password-file file:///run/secrets/IPTS_PASSWORD
        --query "${QUERY}"
        --hcatalog-database "$DB_NAME"
        --hcatalog-table "$TABLE"
    )

    # Create the Hive tables necessary to run sqoop jobs
    # Table definitions are stored in the repo
    hive -f /tmp/tables/"$TABLE".sql

    # Create a sqoop job for the selected table(s). Tables with TAXYR
    # get partitioning and splitby during sqoop import
    if [[ "$CONTAINS_TAXYR" == TRUE ]]; then
        sqoop "${SQOOP_OPTIONS_MAIN[@]}" \
            --boundary-query "${BOUNDARY_QUERY}" \
            --split-by TAXYR \
            --num-mappers "$NUM_MAPPERS"
    else
        sqoop "${SQOOP_OPTIONS_MAIN[@]}" -m 1
    fi

    # Execute saved sqoop job
    echo "Running job for table: ${TABLE} with query: ${QUERY}"
    sqoop job \
        -D java.security.egd=file:///dev/./urandom \
        -D mapred.child.java.opts="-Djava.security.egd=file:///dev/./urandom" \
        --exec "$TABLE"

    # If buckets are specified, rewrite output from sqoop to bucketed table
    # Then copy from distributed file system (HDFS) to local mounted dir
    mkdir -p /tmp/target/"$TABLE"
    if [[ "$NUM_BUCKETS" -gt 1 ]]; then
        if [[ "$CONTAINS_TAXYR" == TRUE ]]; then
            hive -e \
                "INSERT OVERWRITE TABLE ${DB_NAME}.${TABLE_LC}_bucketed
                 PARTITION(taxyr) SELECT * FROM ${DB_NAME}.${TABLE_LC};"
        else
            hive -e \
                "INSERT OVERWRITE TABLE ${DB_NAME}.${TABLE_LC}_bucketed
                 SELECT * FROM ${DB_NAME}.${TABLE_LC};"
        fi
        hdfs dfs -copyToLocal \
            /user/hive/warehouse/"$DB_NAME".db/"$TABLE_LC"_bucketed/* \
            /tmp/target/"$TABLE"
    else
        hdfs dfs -copyToLocal \
            /user/hive/warehouse/"$DB_NAME".db/"$TABLE_LC"/* \
            /tmp/target/"$TABLE"
    fi

    # Remove tables in HDFS once moved to local
    hdfs dfs -rm -r -f /user/hive/warehouse/"$DB_NAME".db/"$TABLE_LC"_bucketed
    hdfs dfs -rm -r -f /user/hive/warehouse/"$DB_NAME".db/"$TABLE_LC"

    # Remove _SUCCESS files and rename parts to parquet
    find /tmp/target/"$TABLE" -type f -name '_SUCCESS' -delete
    find /tmp/target/"$TABLE" -type f -exec mv {} {}.snappy.parquet \;

    # Clear the sqoop workers cache of copied JARs
    rm -rf /tmp/hadoop-root/nm-local-dir/usercache/*
    echo "Completed job for table: ${TABLE}"
done
