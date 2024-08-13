import argparse
from datetime import datetime

from joblib import Parallel, delayed

from utils.helpers import load_job_definitions, load_predicates
from utils.spark import SharedSparkSession, SparkJob

# Default values for jobs, used per job if not explicitly set in the job's
# input JSON. CUR and YEAR values are used for partitioning and filtering
# if they are set
DEFAULT_VAR_CUR = ["Y", "N", "D"]
DEFAULT_VAR_MIN_YEAR = 1999
DEFAULT_VAR_MAX_YEAR = datetime.now().year
DEFAULT_VAR_PREDICATES_PATH = "default_predicates.sql"

# Constants for paths inside the Spark container
PATH_IPTS_PASSWORD = "/run/secrets/IPTS_PASSWORD"
PATH_INITIAL_DIR = "/tmp/target/initial"
PATH_FINAL_DIR = "/tmp/target/final"

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


def main() -> str:
    # Get the job definition(s) from the argument JSON
    args = parse_args()
    job_definitions = load_job_definitions(
        json_file=args.json_file, json_string=args.json_string
    )

    # Each session is shared across all read jobs and manages job order,
    # credentialing, retries, etc.
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    session_name = f"iasworld_{current_datetime}"
    session = SharedSparkSession(
        app_name=session_name, password_file_path=PATH_IPTS_PASSWORD
    )

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

        # Run the Spark read job. Each job will create N predicates
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
    session.spark.stop()

    # Use PyArrow to condense the many small Parquet files created by the reads
    # into single files per Hive partition. This step is sequential since it
    # uses a ton of memory
    for job in jobs:
        job.repartition()

    return session_name


if __name__ == "__main__":
    main()
