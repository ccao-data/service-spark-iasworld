import argparse
import time
from datetime import datetime, timedelta

from joblib import Parallel, delayed
from utils.aws import AWSClient
from utils.github import GitHubClient
from utils.helpers import (
    PATH_SPARK_LOG,
    create_python_logger,
    load_job_definitions,
    load_predicates,
)
from utils.spark import SharedSparkSession, SparkJob

logger = create_python_logger(__name__)

# Default values for jobs, used per job if not explicitly set in the job's
# input JSON. CUR and YEAR values are used for partitioning and filtering
# if they are set
DEFAULT_VAR_MIN_YEAR = 1999
DEFAULT_VAR_MAX_YEAR = datetime.now().year
DEFAULT_VAR_CUR = ["Y", "N", "D"]
DEFAULT_VAR_PREDICATES_PATH = "default_predicates.sql"

# Constants for paths inside the Spark container
PATH_IPTS_PASSWORD = "/run/secrets/IPTS_PASSWORD"
PATH_INITIAL_DIR = "/tmp/target/initial"
PATH_FINAL_DIR = "/tmp/target/final"
PATH_GH_PEM = "/run/secrets/GH_PEM"

# Number of Spark read jobs to run in parallel. Limited to a small number just
# so more jobs can start while single node jobs are running; most of the actual
# parallelism happens within each job
NUM_PARALLEL_JOBS = 4


def parse_args():
    parser = argparse.ArgumentParser(
        description="Submit iasWorld Spark extraction jobs"
    )
    parser.add_argument(
        "--json-file",
        type=str,
        nargs="?",
        help="""
        Path to a JSON file containing job configuration(s)". The path is
        relative to the project `config/` directory
        """,
    )
    parser.add_argument(
        "--json-string",
        type=str,
        nargs="?",
        help="JSON string containing job configurations(s)",
    )

    return parser.parse_args()


def submit_jobs(
    app_name: str,
    json_file: str | None = None,
    json_string: str | None = None,
) -> None:
    """
    Submit Spark jobs for iasWorld data extraction and upload.

    Args:
        app_name: Name of the job batch. Shown in the Spark UI and used as
            the logstream name in CloudWatch.
        json_file: Path to a JSON file containing job configuration(s).
        json_string: JSON string containing job configuration(s).
    """
    # Get the job definition(s) from the argument JSON
    time_start = time.time()
    job_definitions = load_job_definitions(json_file, json_string)

    # Each session is shared across all read jobs and manages job order,
    # credentialing, retries, etc.
    session = SharedSparkSession(
        app_name=app_name,
        password_file_path=PATH_IPTS_PASSWORD,
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

        spark_job = SparkJob(
            session=session,
            table_name=job_definition.get("table_name"),
            taxyr=years,
            cur=cur,
            predicates=predicates,
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
    aws = AWSClient()
    new_local_files = []
    for job in jobs:
        job_upload_results = job.upload(aws)
        new_local_files.extend(job_upload_results)

    # If any jobs uploaded never-seen-before files, trigger a Glue crawler
    if new_local_files:
        logger.info(
            (
                f"{len(new_local_files)} previously unseen files uploaded to S3, "
                "triggering Glue crawler"
            )
        )
        aws.run_and_wait_for_crawler("ccao-data-warehouse-iasworld-crawler")

    # Trigger a GitHub workflow to run dbt tests once all jobs are complete
    logger.info("All file uploads complete, triggering dbt tests")
    github = GitHubClient(gh_pem_path=PATH_GH_PEM)
    github.run_workflow(
        repository="https://api.github.com/repos/ccao-data/data-architecture",
        workflow="test_dbt_models.yaml",
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
    app_name = f"iasworld_{current_datetime}"
    args = parse_args()
    try:
        submit_jobs(app_name, args.json_file, args.json_string)
    except Exception as e:
        logger.error(e)

    aws = AWSClient()
    aws.upload_logs_to_cloudwatch(
        log_group_name="/ccao/jobs/spark",
        log_stream_name=app_name,
        log_file_path=PATH_SPARK_LOG,
    )
