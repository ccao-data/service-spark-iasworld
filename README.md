# Spark Extractor for iasWorld

This repository contains the dependencies and code necessary to run
[Spark](https://spark.apache.org/docs/latest/) extract jobs targeting the
CCAO's iasWorld system-of-record. It is a replacement for
[`service-sqoop-iasworld`](https://github.com/ccao-data/service-sqoop-iasworld),
which is now deprecated.

The Spark jobs pull iasWorld tables (or parts of tables) via
[JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) and
write them as [Hive-partitioned](https://duckdb.org/docs/data/partitioning/hive_partitioning.html)
Parquet files to [AWS S3](https://aws.amazon.com/s3/). The Data Department then
queries these Parquet files using [AWS Athena](https://aws.amazon.com/athena),
giving us a 1-1 mirror of the system-of-record for analytical queries.

## Submitting jobs

> [!NOTE]
> Before attempting to submit jobs to the cluster, first make sure the Spark
> Docker Compose stack is active by running `docker compose up -d` in the
> repository.

`service-spark-iasworld` jobs are submitted via JSON, either as a string or
as a file. All jobs should have the following format:

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
    ...
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

Long-lived job definitions are stored as YAML in `config/default_jobs.yaml`,
then converted to JSON for submission. See `run.sh` for an example of this
workflow using `yq`.

### Predicates, filtering, and partitioning

- **Predicates** are SQL statements used to chunk a table during reads
  against the iasWorld database. The statements define mutually exclusive
  queries that run in parallel (in order to speed up query execution).

  Predicates are defined via a file of SQL statements in the `config/`
  directory, then passed to each table job via a file path.
- **Filters** are logic conditions included in queries to the database. Spark
  uses [predicate pushdown](https://airbyte.com/data-engineering-resources/predicate-pushdown)
  to compose the _predicates_ and _filter_ for each query into a single SQL
  statement. Think of filters as a SQL WHERE clause applied across all the
  predicate chunks specified above.

  Filters are constructed automatically from any `min_year`, `max_year`,
  and/or `cur` values passed as part of a job definition. If these values are
  all null, then the entire table is returned.
- **Partitions** define how the output Parquet files returned from each should
  be broken up. We use Hive partitioning by default, which yields partitions
  with the structure `$TABLE/taxyr=$YEAR/cur=$CUR_VALUE/part-0.parquet`.

  Like filters, partitions are determined automatically via any `min_year`,
  `max_year`, and/or `cur` values that are set. If these values are all null,
  then the table is returned as a single file e.g. `$TABLE/part-0.parquet`.

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
│   ├── default_jobs.yaml      - Definitions used to configure Spark jobs per table
│   ├── default_predicates.sql - List of mutually exclusive SQL BETWEEN expressions
│   └── spark-defaults.conf    - Spark memory and driver settings
├── drivers/
│   └── ojdbc8.jar             - Not included, but necessary to connect to iasWorld
├── secrets/
│   └── IPTS_PASSWORD          - Password file loaded at runtime into containers
├── src/
│   ├── submit_jobs.py         - Job submission entrypoint. Takes JSON as input
│   ├── submit.sh              - Helper to launch jobs using spark-submit
│   └── utils/
│       ├── helpers.py         - Miscellaneous helper files
│       └── spark.py           - Spark job and session classes
└── target/
    ├── final/                 - Landing directory after Parquet repartitioning
    └── initial/               - Landing directory for initial JDBC read output
```
