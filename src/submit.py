import argparse
import json
from datetime import datetime
from utils.helpers import construct_predicates, read_predicates
from utils.spark import SharedSparkSession, SparkJob

# Default values for jobs. CUR and YEAR values are only used if
# USE_PARTITIONS is true. USE_PREDICATES should be disabled for any table
# without a PARID column
DEFAULT_VAR_CUR = ["Y", "N", "D"]
DEFAULT_VAR_MIN_YEAR = 1999
DEFAULT_VAR_MAX_YEAR = datetime.now().year
DEFAULT_VAR_USE_PREDICATES = True
DEFAULT_VAR_USE_PARTITIONS = True

# Constants for paths WITHIN the Spark container
PATH_IPTS_PASSWORD = "/run/secrets/IPTS_PASSWORD"
PATH_PREDICATES = "/tmp/src/predicates.csv"
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
    args = parse_args()

    if args.json_file and args.json_string:
        raise ValueError(
            "Only one argument: --yaml-file or --json-string can be provided"
        )
    elif args.json_file:
        with open(args.json_file, "r") as f:
            job_config = json.load(f)
    elif args.json_string:
        job_config = json.loads(args.json_string)
    else:
        raise ValueError(
            "Either --json-file or --json-string must be provided"
        )

    current_datetime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    predicates_csv = read_predicates(PATH_PREDICATES)

    session_name = f"iasworld_{current_datetime}"
    session = SharedSparkSession(
        app_name=session_name, password_file_path=PATH_IPTS_PASSWORD
    )

    jobs = []
    for job_name, config in job_config.items():
        if config.get("use_partitions", DEFAULT_VAR_USE_PARTITIONS):
            min_year = config.get("min_year", DEFAULT_VAR_MIN_YEAR)
            max_year = config.get("max_year", DEFAULT_VAR_MAX_YEAR)
            cur = config.get("cur", DEFAULT_VAR_CUR)
            years = [x for x in range(min_year, max_year + 1)]
        else:
            years, cur = None, None

        if config.get("use_predicates", DEFAULT_VAR_USE_PREDICATES):
            predicates = construct_predicates(predicates_csv, years)
        else:
            predicates = None

        spark_job = SparkJob(
            session=session,
            table_name=config.get("table_name"),
            taxyr=years,
            cur=cur,
            predicates=predicates,
            initial_dir=PATH_INITIAL_DIR,
            final_dir=PATH_FINAL_DIR,
        )

        spark_job.read()
        jobs.append(spark_job)

    session.spark.stop()

    for config in jobs:
        config.repartition()

    print(session_name)

    return session_name


if __name__ == "__main__":
    main()

# TODO: This becomes "validate and submit jobs"
# Order of ops: validate, run spark jobs, run repartition, upload
# args: json of job(s), upload bool, repartition bool, glue/GH bool

# Separate script used to submit all jobs, wait for spark to finish, then
# use pyarrow to repartition and boto to upload
