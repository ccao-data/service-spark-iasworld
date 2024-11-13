# Spark Extractor for iasWorld

This repository contains the dependencies and code necessary to run
[Spark](https://spark.apache.org/docs/latest/) extract jobs targeting Cook
County's iasWorld property system-of-record. It is a replacement for
[`service-sqoop-iasworld`](https://github.com/ccao-data/service-sqoop-iasworld),
which is now deprecated.

Each Spark job pulls an iasWorld table (or part of a table) via
[JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) and
writes it as [Hive-partitioned](https://duckdb.org/docs/data/partitioning/hive_partitioning.html)
Parquet files to [AWS S3](https://aws.amazon.com/s3/). The Data Department then
queries the Parquet files using [AWS Athena](https://aws.amazon.com/athena),
providing a 1-1 mirror of the system-of-record for analytical queries.

Jobs are submitted in "batches" (called applications by Spark). Each batch may
contain multiple extract jobs. Once all jobs for a batch are complete, we also
(optionally) trigger four additional processes. In order:

1. Upload the extracted Parquet files to AWS S3. Uploads to the bucket and
   prefix specified in the `.env` file.
2. Run an AWS Glue crawler to update table data types and/or partitions in
   the Glue data catalog (which powers Athena). This process only occurs if
   _new_ files are uploaded i.e. those not previously seen on S3.
3. Run a [dbt testing workflow](https://github.com/ccao-data/data-architecture/blob/master/.github/workflows/test_dbt_models.yaml)
   on GitHub Actions. This automatically tests the iasWorld data for issues and
   outputs the results to various tables and reports.
4. Upload the final logs to AWS CloudWatch.

## Submitting job batches

> [!NOTE]
> Before attempting to submit batches to the cluster, first make sure the Spark
> Docker Compose stack is active by running `docker compose up -d` in the
> repository. Also, make sure all secret and `.env` files are populated, see
> [Files not included](#files-not-included) for more information.

`service-spark-iasworld` job batches are submitted via JSON, either as a string
or as a file. All batches should have the format below. Note that the name
of the job itself (e.g. `job2` below) is arbitrary.

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
  prefixed with `iasworld.` (or `ias.` for test environment).
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

The example batch above contains two separate jobs, one per table. If you want
to add additional tables/jobs to the batch, you can manually add the
corresponding table objects and modify the fields as
[listed above](#field-definitions).

In practice, modifying JSON is a bit of a pain, so we store long-lived
batch and job definitions in YAML, then convert them to JSON using `yq`.
The file `config/default_jobs.yaml` contains definitions for three common job
batches:

1. A daily batch that pulls the most recent 2 years of each critical table.
2. A weekend batch that pulls _all_ tables and years.
3. A test batch that pulls a subset of tables with representative situations.

### Submitting via the command line

Batches are submitted to the Spark Docker cluster via the command line. The
main job submission argument is either `--json-string` or `--json-file`.
For example, to submit the test jobs in `config/default_jobs.yaml` via
`--json-string`, run the following command:

```bash
docker exec spark-node-master-prod ./submit.sh \
    --json-string "$(yq -o=json .test_jobs ./config/default_jobs.yaml)"
```

Or from a file:

```bash
yq -o=json .test_jobs ./config/default_jobs.yaml > /tmp/jobs.json
docker exec spark-node-master-prod ./submit.sh --json-file /tmp/jobs.json
```

The command line interface also has multiple optional flags:

- `--extract-target` - iasWorld target environment to extract data from. Must
  be one of `prod` or `test`. Defaults to `prod`.
- `--run-github-workflow/--no-run-github-workflow` - Run the [`test_dbt_models`](https://github.com/ccao-data/data-architecture/blob/master/.github/workflows/test_dbt_models.yaml)
  workflow on batch completion?
- `--run-glue-crawler/--no-run-glue-crawler` - Run the iasWorld Glue crawler
  on batch completion?
- `--upload-data/--no-upload-data` - Upload extracted data to the iasWorld S3
  bucket?
- `--upload-logs/--no-upload-logs` - Upload batch logs to AWS CloudWatch?

The default values for these flags are set in the `config/default_settings.yaml`
file. The boolean flags are all `False` by default.

## Additional notes

### Data types

Spark automatically attempts to mirror the data types within iasWorld using
its own equivalent types. However, on occasion, it may use an incorrect or
undesirable type. In such cases, this repository provides a hierarchical system
of column-level schema/type overrides, with each type overriding the previous
one:

1. By default, all `NUMBER` Oracle types are converted to `DECIMAL(10,0)`
   and `TIMESTAMP` Oracle types are converted to `STRING`. This behavior is
   ignored for columns with an override specified via the options below.
2. Global schema overrides apply to all columns of a given name across all
   tables. They can be specified even for columns that do not exist in every
   table. They are defined in `config/default_settings.yaml`.
3. Table schema overrides apply only to the columns of a single table. They
   take precedence over all other overrides. They are defined in
   `config/table_definitions.yaml`.

> [!WARNING]
> `NUMERIC` types are implicitly converted to `DECIMAL(10,0)` because as of
> 2024, all `NUMERIC` columns without a specified precision and scale are
> actually just integers. If this changes in the future, it's possible that
> we could begin to silently truncate numbers via this implicit type
> conversion. As such, stay on top of schema updates from the iasWorld team.

### Constructing jobs

Predicates, filters, and partitions are Spark concepts used to construct
individual jobs in a batch. They are mostly handled automatically, but you
may need to change them in rare cases. The list below outlines the role of each
concept and how to change them if needed:

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

### Files not included

Some necessary setup and credential files are not included in this repository
for security or licensing reasons. Templated versions are included for
instructional purposes. If you want to use this repository, you will need to
populate the following:

- `drivers/ojdbc8.jar` - This is the JDBC driver for our Oracle backend and
  can be found for free on [Oracle's site](https://www.oracle.com/ca-en/database/technologies/appdev/jdbc-downloads.html).
- `secrets/` - These are credential files needed to connect to other systems.
- `.env` - This file sets a few non-critical but still private options.

## Using the development environment

The Docker Compose stack we use to run Spark (via `docker compose up -d`)
has a separate development environment that can be used to test code changes
without disrupting the production containers.

The development environment uses
[Compose profiles](https://docs.docker.com/compose/how-tos/profiles/) to run
a parallel set of containers with tweaked setup/environment variables.

To start the development environment, run:

```bash
docker compose --profile dev up -d
```

To submit a job to the development environment, change the container target
of your command from `prod` to `dev`. For example:

```bash
docker exec -it spark-node-master-dev ./submit.sh \
    --json-string "$(yq -o=json .test_jobs ./config/default_jobs.yaml)"
```

A typical development workflow might look something like:

1. Clone the repository to your own machine or home directory. Do _not_ use
  the production `shiny-server` copy of the repository for development.
2. Copy the secrets, environmental variables, and drivers from the production
  setup to the development repository. See [Files not included](#files-not-included).
3. Export `UID` and `GID` using something like the command below. This will
  set the user for the container so the output files have the correct permissions.
    ```bash
    export UID=$(id -u)
    export GID=$(id -g)
    ```
4. Build the Docker container targeting the `dev` tag by running
  `docker compose --profile dev build`.
5. Start the development environment using `docker compose --profile dev up -d`.
6. Make your code modifications. Changes in the `src/` directory are reflected
  in the dev containers due to volume mounts (no need to rebuild).
7. Submit a job to the development containers using `docker exec`, targeting
  the development master node (`spark-node-master-dev`).
8. Check the job status at `$SERVER_IP:8081`, instead of the production port
  `$SERVER_IP:8080`.

> [!WARNING]
> The development environment shares the same targets as the production
> environment. That means it will write to the same S3 bucket/CloudWatch log
> group and trigger the same workflows/crawlers (though all these features are
> disabled by default). As such, use this environment carefully. If you mess up
> production data, you can run the production version of the code to re-fetch it.

## Scheduling

Batches are currently scheduled via
[`cron`](https://man7.org/linux/man-pages/man8/cron.8.html). To edit the
schedule file, use `crontab -e` as the main server user. The example crontab
file below schedules daily jobs for frequently updated tables and weekly ones
for rarely-updated tables. Note that the jobs currently _must_ be run as
user 1003.

```bash
# Extract recent years from frequently used tables on weekdays at 1 AM CST
0 6 * * 1,2,3,4,5 docker exec spark-node-master-prod ./submit.sh --json-string "$(yq -o=json .default_jobs /full/path/to/default_jobs.yaml)"

# Extract all tables on Saturday at 1 AM CST
0 6 * * 6 docker exec spark-node-master-prod ./submit.sh --json-string "$(yq -o=json .weekend_jobs /full/path/to/default_jobs.yaml)"

# Extract all test environment tables on Sunday at 1 AM CST
0 6 * * 7 docker exec spark-node-master-prod ./submit.sh --json-string "$(yq -o=json .weekend_jobs_test /full/path/to/default_jobs.yaml)" --no-run-github-workflow --extract-target test
```

## Structure

Here's a breakdown of important files and the purpose of each one:

```tree
.
├── docker-compose.yaml        # Defines the Spark nodes, environment, and networking
├── Dockerfile                 # Defines dependencies bundled in each Spark node
├── .env                       # Runtime configuration variables passed to containers
├── pyproject.toml             # Project metadata and tool settings
├── README.md                  # This file!
├── run.sh                     # Entrypoint shell script to create Spark jobs
├── .github/                   # GitHub Actions workflows for linting, builds, etc.
├── config/
│   ├── default_jobs.yaml      # Define batches of Spark jobs (one per table)
│   ├── default_predicates.sql # List of mutually exclusive SQL BETWEEN expressions
│   ├── default_settings.yaml  # Runtime defaults and schema overrides
│   ├── spark-defaults.conf    # Spark memory and driver settings
│   └── table_definitions.yaml # Possible job values per table and schema overrides
├── drivers/
│   └── ojdbc8.jar             # Not included, but necessary to connect to iasWorld
├── secrets/
│   ├── AWS_CREDENTIALS_FILE   # AWS credentials config file specific to this job
│   ├── GH_PEM                 # GitHub PEM file used to authorize workflow dispatch
│   └── IPTS_PASSWORD          # Password file loaded at runtime into containers
├── src/
│   ├── submit_jobs.py         # Job submission entrypoint. Takes JSON as input
│   ├── submit.sh              # Helper to launch jobs using spark-submit
│   └── utils/
│       ├── aws.py             # AWS client class for triggering Glue crawlers
│       ├── github.py          # GitHub client class for running Actions workflows
│       ├── helpers.py         # Miscellaneous helper functions
│       └── spark.py           # Spark job and session classes
└── target/
    ├── final/                 # Landing directory after Parquet repartitioning
    └── initial/               # Landing directory for initial JDBC read output
```
