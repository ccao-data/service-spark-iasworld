import argparse
from datetime import datetime

from utils.helpers import construct_predicates, load_job_config
from utils.spark import SharedSparkSession, SparkJob

# Default values for jobs, used per job if not explicitly set in the job's
# input JSON. CUR and YEAR values are only used if use_partitions is true in
# the job definition.
DEFAULT_VAR_CUR = ["Y", "N", "D"]
DEFAULT_VAR_MIN_YEAR = 1999
DEFAULT_VAR_MAX_YEAR = datetime.now().year
DEFAULT_VAR_PREDICATES_PATH = "default_predicates.csv"
DEFAULT_VAR_PREDICATES_TYPE = "string"
DEFAULT_VAR_USE_PARTITIONS = True

# Constants for paths inside the Spark container
PATH_IPTS_PASSWORD = "/run/secrets/IPTS_PASSWORD"
PATH_INITIAL_DIR = "/tmp/target/initial"
PATH_FINAL_DIR = "/tmp/target/final"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Submit iasWorld Spark extraction jobs"
    )
    parser.add_argument(
        "--json-file",
        type=str,
        nargs="?",
        help="Path to a JSON file containing job configuration(s)",
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
    job_config = load_job_config(args)

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
    for job_name, config in job_config.items():
        use_partitions = config.get(
            "use_partitions", DEFAULT_VAR_USE_PARTITIONS
        )
        if use_partitions:
            min_year = config.get("min_year", DEFAULT_VAR_MIN_YEAR)
            max_year = config.get("max_year", DEFAULT_VAR_MAX_YEAR)
            cur = config.get("cur", DEFAULT_VAR_CUR)
            years = [x for x in range(min_year, max_year + 1)]
        else:
            years, cur = None, None

        if config.get("predicates_path", DEFAULT_VAR_PREDICATES_PATH):
            predicates = construct_predicates(
                DEFAULT_VAR_PREDICATES_PATH, DEFAULT_VAR_PREDICATES_TYPE
            )
        else:
            predicates = None

        spark_job = SparkJob(
            session=session,
            table_name=config.get("table_name"),
            taxyr=years,
            cur=cur,
            predicates=predicates,
            use_partitions=use_partitions,
            initial_dir=PATH_INITIAL_DIR,
            final_dir=PATH_FINAL_DIR,
        )

        # Run the Spark read job. Each job will create n_predicates X n_years
        # files in the target/initial/ dir, assuming predicates are enabled
        spark_job.read()
        jobs.append(spark_job)

    # Stop the Spark session once all JDBC reads are complete. This is CRITICAL
    # as it frees all memory from the cluster for use in the repartition step
    session.spark.stop()

    # Use pyarrow to condense the many small Parquet files created by the reads
    # into single files per Hive partition
    for config in jobs:
        config.repartition()

    return session_name


if __name__ == "__main__":
    main()
