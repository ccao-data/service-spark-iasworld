# Spark Extractor for iasWorld

This repository contains the dependencies and code necessary to run
[`Spark`](https://spark.apache.org/docs/latest/) extract jobs targeting the
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
> compose stack is active by running `docker compose up -d` in the repository.

`service-spark-iasworld` jobs are submitted via JSON, either as a string or
as a file. All jobs should have the following format:

```json
{
  "addn": {
    "table_name": "iasworld.addn",
    "min_year": 2020,
    "max_year": 2024,
    "cur": ["Y", "D"],
    "use_predicates": false,
    "use_partitions": false
  },
  "job2": {
    ...
  }
}
```

### Field definitions

- `table_name (required)` - Name of the iasWorld table to extract, must be
  prefixed with `iasworld.`.
- `min_year (optional)` - Minimum tax year to extract from the table.
  Default is `1999`.
- `max_year (optional)` - Maximum tax year (inclusive) to extract from the
  table. Default is the current year. To extract a single year, set `min_year`
  and `max_year` to the same value.
- `cur (optional)` - Values of the `cur` column to extract from the table. Can
  by an array or a single value. Default is `['Y', 'N', 'D']`.
- `use_predicates (optional)` - Use the PIN ranges defined in
  `config/default_predicates.csv` to divide up the table during extraction.
  Greatly increases speed, but requires the table to have a `parid` column.
  Default is `true`.
- `use_partitions (optional)` - Create
  [Hive-partitioned](https://duckdb.org/docs/data/partitioning/hive_partitioning.html)
  outputs using the values of `taxyr` and `cur`. Default is `true`.

Long-lived job definitions are stored as YAML in `config/default_jobs.yaml`,
then converted to JSON for submission. See `run.sh` for an example of this
workflow using `yq`.

## Structure

Here's a breakdown of important files and the purpose of each one:

```tree
.
├── docker-compose.yaml        - Defines the Spark nodes, their environment, and networking
├── Dockerfile                 - Defines dependencies bundled in each Spark node
├── .env                       - Runtime configuration variables passed to containers
├── pyproject.toml             - Project metadata and tool settings
├── README.md                  - This file!
├── run.sh                     - Entrypoint shell script to create Spark jobs
├── .github/                   - GitHub Actions workflows for linting, builds, etc.
├── config/
│   ├── default_jobs.yaml      - Definitions used to configure Spark jobs per table
│   ├── default_predicates.csv - A list of start/end PINs defining equally sized chunks
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
