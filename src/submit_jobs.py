import argparse
import os
import time
from datetime import datetime, timedelta

from joblib import Parallel, delayed

from utils.aws import AWSClient
from utils.github import GitHubClient
from utils.helpers import (
    PATH_SPARK_LOG,
    clear_directory,
    create_python_logger,
    load_job_definitions,
    load_predicates,
    load_yaml,
    strip_table_prefix,
)
from utils.spark import SharedSparkSession, SparkJob

# Clear any existing log file immediately on session start
if os.path.exists(PATH_SPARK_LOG):
    os.remove(PATH_SPARK_LOG)

logger = create_python_logger(__name__)

# Default values for jobs, used per job if not explicitly set in the job's
# input JSON. CUR and YEAR values are used for partitioning and filtering
# if they are set
DEFAULT_VAR_MIN_YEAR = 1800
DEFAULT_VAR_MAX_YEAR = datetime.now().year
DEFAULT_VAR_CUR = ["Y", "N", "D"]
DEFAULT_VAR_PREDICATES_PATH = "default_predicates.sql"

# Constants for paths inside the Spark container
PATH_INITIAL_DIR = "/tmp/target/initial"
PATH_FINAL_DIR = "/tmp/target/final"
PATH_GH_PEM = "/run/secrets/GH_PEM"
PATH_DEFAULT_SETTINGS = "/tmp/config/default_settings.yaml"
PATH_TABLE_DEFINITIONS = "/tmp/config/table_definitions.yaml"

# Number of Spark read jobs to run in parallel. Limited to a small number just
# so more jobs can start while single node jobs are running; most of the actual
# parallelism happens within each job
NUM_PARALLEL_JOBS = 4


def parse_args(defaults) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit iasWorld Spark extraction jobs"
    )
    parser.add_argument(
        "--extract-target",
        type=str,
        choices=["test", "prod"],
        default=defaults.get("extract_target", "prod"),
        help="Which iasWorld environment to extract data from",
    )
    parser.add_argument(
        "--json-file",
        type=str,
        nargs="?",
        help=(
            "Path to a JSON file containing job configuration(s). The path is "
            "relative to the project `config/` directory."
        ),
    )
    parser.add_argument(
        "--json-string",
        type=str,
        nargs="?",
        help="JSON string containing job configurations(s).",
    )
    parser.add_argument(
        "--run-github-workflow",
        action=argparse.BooleanOptionalAction,
        required=False,
        default=defaults.get("run_github_workflow", False),
        help="Run GitHub Actions workflow to run dbt tests. Defaults to False.",
    )
    parser.add_argument(
        "--run-glue-crawler",
        action=argparse.BooleanOptionalAction,
        required=False,
        default=defaults.get("run_glue_crawler", False),
        help=(
            "Run Glue crawler to discover new partitions. Only runs if "
            "previously unseen files are uploaded to S3. Defaults to False."
        ),
    )
    parser.add_argument(
        "--upload-logs",
        action=argparse.BooleanOptionalAction,
        required=False,
        default=defaults.get("upload_logs", False),
        help="Toggle upload of job logs to AWS CloudWatch. Defaults to False.",
    )
    parser.add_argument(
        "--upload-data",
        action=argparse.BooleanOptionalAction,
        required=False,
        default=defaults.get("upload_data", False),
        help=(
            "Toggle upload of extracted files to AWS S3. If set to False, "
            "files will still be created locally. Defaults to False."
        ),
    )

    return parser.parse_args()


def submit_jobs(
    app_name: str,
    extract_target: str,
    json_file: str | None = None,
    json_string: str | None = None,
    run_github_workflow: bool = False,
    run_glue_crawler: bool = False,
    upload_data: bool = False,
) -> None:
    """
    Submit Spark jobs for iasWorld data extraction and upload.

    See the `parse_args` function for available command-line arguments.
    """
    # Get the job definition(s) from the argument JSON
    time_start = time.time()
    job_definitions = load_job_definitions(json_file, json_string)

    # Load table definitions and schema overrides from file
    global_schema_overrides = load_yaml(
        PATH_DEFAULT_SETTINGS, "global_schema_overrides"
    )
    table_definitions = load_yaml(PATH_TABLE_DEFINITIONS, "tables")

    # Each session is shared across all read jobs and manages job order,
    # credentialing, retries, etc.
    target = "TST" if extract_target == "test" else "PRD"
    pw_file_path = f"/run/secrets/IPTS_{target}_PASSWORD"
    session = SharedSparkSession(
        app_name=app_name,
        extract_target=extract_target,
        password_file_path=pw_file_path,
    )

    # Perform some startup logging before entering the main job loop
    table_names = [i.get("table_name") for i in job_definitions.values()]
    logger.info(f"Starting Spark session {app_name}")
    logger.info(f"Extracting tables: {', '.join(table_names)}")

    # For each Spark job, get the table structure based on the job definition
    # or defaults. Then, perform the JDBC read of the job to extract data.
    # Finally, append the job to a list so we can use it later
    jobs = []
    for job_name, job_definition in job_definitions.items():
        table_name = job_definition.get("table_name")
        cur = job_definition.get("cur", DEFAULT_VAR_CUR)
        min_year = job_definition.get("min_year", DEFAULT_VAR_MIN_YEAR)
        max_year = job_definition.get("max_year", DEFAULT_VAR_MAX_YEAR)
        predicates_path = job_definition.get(
            "predicates_path", DEFAULT_VAR_PREDICATES_PATH
        )

        # Construct the year range and predicates to use for the job query
        years = (
            [x for x in range(min_year, max_year + 1)]
            if min_year and max_year
            else None
        )
        predicates = (
            load_predicates(predicates_path) if predicates_path else None
        )

        # Combine lists of global and table schema overrides and flatten them
        # into a single dictionary
        table_schema_overrides = table_definitions.get(
            strip_table_prefix(table_name), {}
        ).get("schema_overrides", {})
        schema_overrides = global_schema_overrides.copy()
        schema_overrides.update(table_schema_overrides)

        spark_job = SparkJob(
            session=session,
            table_name=table_name,
            taxyr=years,
            cur=cur,
            predicates=predicates,
            schema_overrides=schema_overrides,
            initial_dir=PATH_INITIAL_DIR,
            final_dir=PATH_FINAL_DIR,
        )

        # Register the job so that we can run them all in parallel once
        # they have all been registered. Each job will create N predicates
        # files in the target/initial/ dir, assuming predicates are enabled
        jobs.append(spark_job)

    # Run all Spark read jobs, using parallelization so that new jobs can
    # start before the previous ones finish. The mini-function is required when
    # using delayed()
    def read_job(job: SparkJob):
        job.read()

    Parallel(n_jobs=NUM_PARALLEL_JOBS, prefer="threads")(
        delayed(read_job)(job) for job in jobs
    )

    # Stop the Spark session once all JDBC reads are complete. This is CRITICAL
    # as it frees all memory from the cluster for use in the repartition step
    logger.info("All extractions complete, shutting down Spark")
    session.spark.stop()

    # Use PyArrow to condense the many small Parquet files created by the reads
    # into single files per Hive partition. This step is sequential since it
    # uses a ton of memory
    for job in jobs:
        job.repartition()

    # Upload extracted files to AWS S3 in Hive-partitioned Parquet
    s3_prefix = "iasworld" if extract_target == "prod" else "iasworld_test"
    aws = AWSClient(s3_prefix=s3_prefix)
    new_local_files = []
    if upload_data:
        for job in jobs:
            job_upload_results = job.upload(aws)
            new_local_files.extend(job_upload_results)

    # If any jobs uploaded never-seen-before files, trigger a Glue crawler
    if new_local_files and run_glue_crawler:
        logger.info(f"Previously unseen files: {new_local_files}")
        logger.info(
            (
                f"{len(new_local_files)} previously unseen files uploaded "
                "to S3, triggering Glue crawler"
            )
        )
        if extract_target == "prod":
            crawler_name = "ccao-data-warehouse-iasworld-crawler"
        else:
            crawler_name = "ccao-data-warehouse-iasworld_test-crawler"
        aws.run_and_wait_for_crawler(crawler_name)

    # Trigger a GitHub workflow to run dbt tests once all jobs are complete
    if run_github_workflow:
        logger.info("All file uploads complete, triggering dbt tests")
        github = GitHubClient(gh_pem_path=PATH_GH_PEM)
        github.run_workflow(
            repository=(github.gh_api_url + "ccao-data/data-architecture"),
            workflow="test_iasworld_data.yaml",
            inputs={
                "upload_test_results": True,
            },
        )

    # Print table names and descriptions for extracted tables
    logger.info(f"Extracted tables: {', '.join(table_names)}")
    logger.info("Extracted tables using the following settings:")
    for job in jobs:
        logger.info(job.get_description())

    time_end = time.time()
    time_duration = str(timedelta(seconds=(time_end - time_start)))
    logger.info(f"Total extraction duration was {time_duration}")


if __name__ == "__main__":
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    default_args = load_yaml(PATH_DEFAULT_SETTINGS, "default_args")
    args = parse_args(default_args)
    logger.info(f"Starting Spark application with arguments: {args}")
    app_name = f"iasworld_{args.extract_target}_{current_datetime}"

    try:
        # Clear the existing extract files before submitting new jobs.
        # This is to prevent new jobs from becoming mixed with the results of
        # previous failed or cancelled jobs
        clear_directory(PATH_INITIAL_DIR)
        clear_directory(PATH_FINAL_DIR)

        submit_jobs(
            app_name=app_name,
            extract_target=args.extract_target,
            json_file=args.json_file,
            json_string=args.json_string,
            run_github_workflow=args.run_github_workflow,
            run_glue_crawler=args.run_glue_crawler,
            upload_data=args.upload_data,
        )
    except Exception as e:
        logger.error(e)

    if args.upload_logs:
        aws = AWSClient()
        aws.upload_logs_to_cloudwatch(
            log_group_name="/ccao/jobs/spark",
            log_stream_name=app_name,
            log_file_path=PATH_SPARK_LOG,
        )
