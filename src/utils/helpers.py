import json
import logging
import yaml
from pathlib import Path

PATH_SPARK_LOG = "/tmp/logs/spark.log"


def create_python_logger(
    name: str, log_file_path: str = PATH_SPARK_LOG
) -> logging.Logger:
    """
    Spark logger from the JVM. Also used as a fallback in case any part of the
    main job loop fails.

    Args:
        name: Module name to use for the logger.
        log_file_path: String path to the log file where logs will be written.

    Returns:
        logging.Logger: Generic logger with the same log format as Spark.
    """

    # Formatter class to change WARNING to WARN for consistency with Spark
    class CustomFormatter(logging.Formatter):
        def format(self, record):
            if record.levelname == "WARNING":
                record.levelname = "WARN"
            return super().format(record)

    file_formatter = CustomFormatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d_%H:%M:%S",
    )
    stdout_formatter = CustomFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%d/%m/%y %H:%M:%S",
    )

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file_path, mode="a")
    file_handler.setFormatter(file_formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(stdout_formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


def load_job_definitions(
    json_file: str | None, json_string: str | None
) -> dict:
    """
    Loads job definition(s) from a JSON file or a JSON string.

    Args:
        json_file: String path to a JSON file containing job configurations
            in the format specified in the README. Path is relative to the
            `config/` directory.
        json_string: JSON string containing job configurations in the format
            specified in the README.

    Raises:
        ValueError: If both `json_file` and `json_string` are provided, or if
            neither is provided.

    Returns:
        dict: The job definition(s) loaded from the JSON file or string.
    """
    if json_file and json_string:
        raise ValueError(
            "Only one argument: --json-file or --json-string can be provided"
        )
    elif json_file:
        full_path = "/tmp/config" / Path(json_file)
        with open(full_path, "r") as file:
            job_definitions = json.load(file)
    elif json_string:
        job_definitions = json.loads(json_string)
    else:
        raise ValueError(
            "Either --json-file or --json-string must be provided"
        )
    return job_definitions


def load_predicates(path: str) -> list[str]:
    """
    Fetch BETWEEN predicates from a provided SQL file.

    Args:
        path: String path to a SQL file within the `config/`
            directory. The SQL file should define SQL BETWEEN expressions,
            where each expression is one chunk that will be extracted by
            Spark during JDBC reads. Expressions should not be overlapping.

    Returns:
        A list of SQL predicate strings used to divide a table into chunks
        during JDBC reads.
    """
    full_path = "/tmp/config" / Path(path)
    with open(full_path.resolve().as_posix(), mode="r") as file:
        predicates = [line.strip() for line in file.readlines()]

    return predicates


def load_yaml(path: str, key: str):
    """
    Fetch values from from a static YAML file.

    Args:
        path: String path to a YAML file within the `config/` directory.
        key: Arbitrary key to fetch values from the YAML file.

    Returns: Values from the corresponding YAML key.
    """
    with open(path, mode="r") as file:
        data = yaml.safe_load(file)
    values = data.get(key)
    return values


def strip_table_prefix(table_name: str) -> str:
    """Removes the 'iasworld.' prefix from a table name."""
    if table_name.startswith("iasworld."):
        return table_name.split(".", 1)[1]
    return table_name
