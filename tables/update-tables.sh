#!/bin/bash
# Script to convert table definitions created by Sqoop into
# bucketed, partitioned tables saved as parquet

# Check if table env var exists, if not, use all tables
if [[ -z "$IPTS_TABLE" ]]; then
    JOB_TABLES=$(awk -F"," 'NR>1 {print $1}' /tmp/tables/tables-list.csv)
else
    JOB_TABLES="$IPTS_TABLE"
fi

# Loop through each file and convert
for TABLE in ${JOB_TABLES}; do
    TABLE_LC=$(echo "$TABLE" | awk '{print tolower($0)}')

    # Boolean value for whether $TABLE contains the TAXYR column
    CONTAINS_TAXYR=$(
        awk -F"," \
            -v table="$TABLE" \
            '$1 == table {print $2}' \
            tables-list.csv \
            | tr -d "\n" | tr -d "\r"
    )

    # Get the number of buckets to use for Hive partitioning
    NUM_BUCKETS=$(
        awk -F"," \
            -v table="$TABLE" \
            '$1 == table {print $3}' \
            tables-list.csv \
            | tr -d "\n" | tr -d "\r"
    )

    # Remove properties and options added by Sqoop and remove taxyr col (it
    # will be readded manually)
    sed '/ROW FORMAT SERDE/Q' "$TABLE".sql \
        | sed "/\`taxyr\` decimal(4,0)/d" \
            > "$TABLE".sql.tmp1

    # Manually update some columns with corrected data types
    for coord in xcoord ycoord zcoord; do
        sed -i "s/\`$coord\` decimal(10,0)/\`$coord\` decimal(15,8)/" "$TABLE".sql.tmp1
    done

    # For TAXYR and BUCKETS, create an unpartitioned, unbucketed table
    # as a temporary place for sqoop to extract to, and a partitioned,
    # bucketed table as a final location
    if [[ "$CONTAINS_TAXYR" == TRUE && "$NUM_BUCKETS" -gt 1 ]]; then

        # Create partitioned bucketed table def
        cp "$TABLE".sql.tmp1 "$TABLE".sql.tmp2
        sed -i "/^CREATE TABLE/s/${TABLE_LC}/${TABLE_LC}\_bucketed/" "$TABLE".sql.tmp2
        echo "PARTITIONED BY (\`taxyr\` string)
CLUSTERED BY (\`parid\`) SORTED BY (\`cur\`) INTO ${NUM_BUCKETS} BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');" \
            | tr -s ' ' \
                >> "$TABLE".sql.tmp2

        # Create table def to extract to
        sed -i '$ s/.$/\,/' "$TABLE".sql.tmp1
        echo "  \`taxyr\` string)
STORED AS RCFILE;
        " >> "$TABLE".sql.tmp1

        cat "$TABLE".sql.tmp1 "$TABLE".sql.tmp2 > "$TABLE".sql

    # For tables with TAXYR but not enough data to bucket, do the above,
    # but remove buckets and temp table (sqoop can extract directly as
    # long as there are no buckets)
    elif [[ "$CONTAINS_TAXYR" == TRUE && ! "$NUM_BUCKETS" -gt 1 ]]; then

        sed '/\`taxyr\`/d' "$TABLE".sql.tmp1 > "$TABLE".sql.tmp2
        echo "PARTITIONED BY (\`taxyr\` string)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');" \
            | tr -s ' ' \
                >> "$TABLE".sql.tmp2

        cat "$TABLE".sql.tmp2 > "$TABLE".sql

    # With buckets and no TAXYR, don't create partitions
    elif [[ "$CONTAINS_TAXYR" == FALSE && "$NUM_BUCKETS" -gt 1 ]]; then

        cp "$TABLE".sql.tmp1 "$TABLE".sql.tmp2
        sed -i "s/${TABLE_LC}/${TABLE_LC}\_bucketed/g" "$TABLE".sql.tmp2
        echo "CLUSTERED BY (\`parid\`) SORTED BY (\`cur\`) INTO ${NUM_BUCKETS} BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');" \
            | tr -s ' ' \
                >> "$TABLE".sql.tmp2

        echo "STORED AS RCFILE;
        " >> "$TABLE".sql.tmp1
        cat "$TABLE".sql.tmp1 "$TABLE".sql.tmp2 > "$TABLE".sql
    else

        echo "STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');" >> "$TABLE".sql.tmp1
        mv "$TABLE".sql.tmp1 "$TABLE".sql

    fi

    # Delete bucketing sort of CV table (no cur)
    if [[ "$TABLE" == CVLEG ]]; then
        sed -i "s/SORTED BY (\`cur\`) //" "$TABLE".sql
    elif [[ "$TABLE" == CVOWN || "$TABLE" == CVTRAN ]]; then
        sed -i "/^CLUSTERED BY/d" "$TABLE".sql
    fi

    # Trim any trailing whitespace
    sed -i "s/[ \t]*$//" "$TABLE".sql

    rm -f ./*.tmp*

done
