import argparse
import json
from datetime import datetime
from utils.helpers import construct_predicates, read_predicates
from utils.spark import SharedSparkSession, SparkJob

PATH_DRIVER = "/jdbc/ojdbc8.jar"
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
        help="Path to the JSON file containing job configuration(s)",
    )
    parser.add_argument(
        "--json-string",
        type=str,
        help="JSON string containing job configurations(s)",
    )

    return parser.parse_args()


def main() -> str:
    args = parse_args()

    if args.json_file and args.json_string:
        raise ValueError(
            "Only one argument: --json-file or --json-string can be provided"
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

    current_year = datetime.now().year
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    predicates_csv = read_predicates(PATH_PREDICATES)

    session_name = f"iasworld_{current_datetime}"
    session = SharedSparkSession(
        app_name=session_name,
        password_file_path=PATH_IPTS_PASSWORD,
        driver_file_path=PATH_DRIVER,
    )

    jobs = []
    for job in job_config["jobs"]:
        min_year = job.get("min_year", 1999)
        max_year = job.get("max_year", current_year)
        if min_year is None and max_year is None:
            years = None
        else:
            years = [x for x in range(min_year, max_year + 1)]

        predicates = construct_predicates(predicates_csv, years)

        spark_job = SparkJob(
            session=session,
            table_name=job.get("table_name"),
            taxyr=years,
            cur=job.get("cur", ["Y", "N", "D"]),
            predicates=predicates,
            initial_dir=PATH_INITIAL_DIR,
            final_dir=PATH_FINAL_DIR,
        )

        spark_job.read()
        jobs.append(spark_job)

    session.spark.stop()
    print(session_name)

    return session_name


if __name__ == "__main__":
    main()

# TODO: This becomes "validate and submit jobs"
# Order of ops: validate, run spark jobs, run repartition, upload
# args: json of job(s), upload bool, repartition bool, glue/GH bool

# Separate script used to submit all jobs, wait for spark to finish, then
# use pyarrow to repartition and boto to upload
