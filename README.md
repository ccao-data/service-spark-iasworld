# Spark Extractor for iasWorld

This repository contains the dependencies and code necessary to run
[Spark](https://spark.apache.org/docs/latest/) extract jobs targeting the
CCAO's iasWorld system-of-record. It is a replacement for
[`service-sqoop-iasworld`](https://github.com/ccao-data/service-sqoop-iasworld),
which is now deprecated.

Each Spark job pulls an iasWorld table (or part of a table) via
[JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) and
writes it as [Hive-partitioned](https://duckdb.org/docs/data/partitioning/hive_partitioning.html)
Parquet files to [AWS S3](https://aws.amazon.com/s3/). The Data Department then
queries the Parquet files using [AWS Athena](https://aws.amazon.com/athena),
giving us a 1-1 mirror of the system-of-record for analytical queries.

Jobs are submitted in "batches" (called applications by Spark). Each batch may
contain multiple extract jobs. Once all jobs for a batch are complete, we also
(optionally) trigger two additional processes:

- Run an AWS Glue crawler to update table data types and/or partitions. This
  only occurs if _new_ files are uploaded i.e. ones not previously seen on S3.
- Run a [dbt testing workflow](https://github.com/ccao-data/data-architecture/blob/master/.github/workflows/test_dbt_models.yaml)
  on GitHub Actions. This automatically tests the new data for issues and
  outputs results to various tables and reports.

## Submitting job batches

> [!NOTE]
> Before attempting to submit batches to the cluster, first make sure the Spark
> Docker Compose stack is active by running `docker compose up -d` in the
> repository. Also, make sure all secret and `.env` files are populated.

`service-spark-iasworld` job batches are submitted via JSON, either as a string
or as a file. All batches should have the following format:

```json
{
  "addn": {
    "table_name": "iasworld.addn",
    "min_year": 2020,
    "max_year": 2024,
    "cur": ["Y", "D"],
    "predicates_path": "default_predicates.sql"
  },
  "job2": {
    "table_name": "iasworld.asmt_all",
    "min_year": 2021,
    "max_year": 2021,
    "cur": ["Y"],
    "predicates_path": "default_predicates.sql"
  }
}
```

### Field definitions

- `table_name (required)` - Name of the iasWorld table to extract, must be
  prefixed with `iasworld.`.
- `min_year (optional)` - Minimum tax year (inclusive) to extract from the
  table. Set to `null` in a job definition to ignore this column when filtering
  and partitioning. Defaults to `1999`.
- `max_year (optional)` - Maximum tax year (inclusive) to extract from the
  table. To extract a single year, set `min_year` and `max_year` to the same
  value. Set to `null` in a job definition to ignore this column when filtering
  and partitioning. Defaults to the current year.
- `cur (optional)` - Values of the `cur` column to extract from the table.
  Must be an array. Set to `null` in a job definition to ignore this column
  when filtering and partitioning. Defaults to `["Y", "N", "D"]`.
- `predicates_path (optional)` - String path to a SQL file within the `config/`
  directory. The SQL file should define SQL BETWEEN expressions, where each
  expression is one chunk that will be extracted by Spark during JDBC reads.
  Expressions should not be overlapping. Set to `null` in a job definition
  to disable using predicates completely. Defaults to `default_predicates.sql`.

### Creating batch JSON

The batch above contains two separate jobs, one per table. If you want to add
additional tables to the batch, you can manually add corresponding table
objects and modify the fields and listed above.

In practice, modifying JSON is a bit of a pain, so we store long-lived
batch/job definitions in YAML, then convert them to JSON using `yq`.
The file `config/default_jobs.yaml` contains definitions for three common job
batches we use: one for daily pulls, one for weekends, and one for testing.

The `./run.sh` script contains an example of using `yq` to submit jobs.

## Additional notes

### Data types

Spark automatically attempts to mirror the data types within iasWorld using
its own equivalent types. However, on occasion, it may use an incorrect or
undesirable type. In such cases, this repo provides a hierarchical system of
column-level schema/type overrides, with each type overriding the previous one:

1. By default, all `NUMBER` Oracle types are converted to `DECIMAL(10,0)`
   and `TIMESTAMP` Oracle types are converted to `STRING`. The behavior is
   ignored if an override is specified via the options below.
2. Global schema overrides apply to all columns of a given name across all
   tables. They can be specified even for columns that do not exist in every
   table. They are defined in `config/default_settings.yaml`.
3. Table schema overrides apply only to the columns of a single table. They
   take precedence over all other overrides. They are defined in
   `config/table_definitions.yaml`.

### Constructing jobs

Predicates, filters, and partitions are Spark concepts used to construct
individual jobs in a batch. They are mostly handled automatically, but you
may need to change them in rare cases. The list below outlines the role of each
concept and how to change them if needed:

- **Predicates** are SQL statements used to chunk a table during reads
  against the iasWorld database. The statements define mutually exclusive
  queries that run in parallel (in order to speed up query execution).<br>
  Predicates are defined via a file of SQL statements in the `config/`
  directory, then passed to each table job via a file path.
- **Filters** are logic conditions included in queries to the database. Spark
  uses [predicate pushdown](https://airbyte.com/data-engineering-resources/predicate-pushdown)
  to compose the _predicates_ and _filter_ for each query into a single SQL
  statement. Think of filters as a SQL WHERE clause applied across all the
  predicate chunks specified above.<br>
  Filters are constructed automatically from any `min_year`, `max_year`,
  and/or `cur` values passed as part of a job definition. If these values are
  all null, then the entire table is returned.
- **Partitions** define how the output Parquet files returned from each should
  be broken up. We use Hive partitioning by default, which yields partitions
  with the structure `$TABLE/taxyr=$YEAR/cur=$CUR_VALUE/part-0.parquet`.<br>
  Like filters, partitions are determined automatically via any `min_year`,
  `max_year`, and/or `cur` values that are set. If these values are all null,
  then the table is returned as a single file e.g. `$TABLE/part-0.parquet`.

## Scheduling

Table extractions are schedule via
[`cron`](https://man7.org/linux/man-pages/man8/cron.8.html). To edit the
schedule file, use `crontab -e`. The example crontab file below schedules daily
jobs for frequently updated tables and weekly ones for rarely-updated tables.
Note that the jobs currently _must_ be run as user 1003.

```bash
# Extract recent years from frequently used tables on weekdays at 1 AM CST
0 6 * * 1,2,3,4,5 docker exec spark-node-master ./submit.sh --json-string "$(yq -o=json .default_jobs /full/path/to/default_jobs.yaml)"

# Extract all tables on Saturday at 1 AM CST
0 6 * * 6 docker exec spark-node-master ./submit.sh --json-string "$(yq -o=json .weekend_jobs /full/path/to/default_jobs.yaml)"
```

## Structure

Here's a breakdown of important files and the purpose of each one:

```tree
.
├── docker-compose.yaml        - Defines the Spark nodes, environment, and networking
├── Dockerfile                 - Defines dependencies bundled in each Spark node
├── .env                       - Runtime configuration variables passed to containers
├── pyproject.toml             - Project metadata and tool settings
├── README.md                  - This file!
├── run.sh                     - Entrypoint shell script to create Spark jobs
├── .github/                   - GitHub Actions workflows for linting, builds, etc.
├── config/
│   ├── default_jobs.yaml      - Define batches of Spark jobs (one per table)
│   ├── default_predicates.sql - List of mutually exclusive SQL BETWEEN expressions
│   ├── default_settings.yaml  - Runtime defaults and schema overrides
│   ├── spark-defaults.conf    - Spark memory and driver settings
│   └── table_definitions.yaml - Possible job values per table and schema overrides
├── drivers/
│   └── ojdbc8.jar             - Not included, but necessary to connect to iasWorld
├── secrets/
│   ├── AWS_CREDENTIALS_FILE   - AWS credentials config file specific to this job
│   ├── GH_PEM                 - GitHub PEM file used to authorize workflow dispatch
│   └── IPTS_PASSWORD          - Password file loaded at runtime into containers
├── src/
│   ├── submit_jobs.py         - Job submission entrypoint. Takes JSON as input
│   ├── submit.sh              - Helper to launch jobs using spark-submit
│   └── utils/
│       ├── aws.py             - AWS client class for triggering Glue crawlers
│       ├── github.py          - GitHub client class for running Actions workflows
│       ├── helpers.py         - Miscellaneous helper functions
│       └── spark.py           - Spark job and session classes
└── target/
    ├── final/                 - Landing directory after Parquet repartitioning
    └── initial/               - Landing directory for initial JDBC read output
```
