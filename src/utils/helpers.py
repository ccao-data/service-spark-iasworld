import json
import logging
from pathlib import Path

import yaml
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType, StringType, TimestampType

PATH_SPARK_LOG = "/tmp/logs/spark.log"


def convert_datetime_columns(
    df: DataFrame, ignore_cols: list[str]
) -> DataFrame:
    """
    iasWorld stores datetimes via the TIMESTAMP data type, but historically
    we converted these columns to strings to avoid messy type incompatibility
    with Athena. This function converts TIMESTAMP columns to UTC datetime
    strings. Columns specified via schema overrides will ignore this conversion.

    Args:
        df: Spark DataFrame with columns to convert.
        ignore_cols: Ignore columns specified in this list during conversion.

    Returns:
        Spark DataFrame with TIMESTAMP columns converted to STRING.
    """
    for field in df.schema.fields:
        if (
            isinstance(field.dataType, TimestampType)
            and field.name not in ignore_cols
        ):
            df = df.withColumn(field.name, df[field.name].cast(StringType()))
    return df


def convert_decimal_columns(
    df: DataFrame, ignore_cols: list[str]
) -> DataFrame:
    """
    iasWorld stores many columns as the generic NUMBER Oracle data type, but
    almost always uses DECIMAL(10,0) for the actual values. This function
    automatically converts all such columns to DECIMAL(10,0). Columns specified
    via schema overrides will ignore this conversion.

    Args:
        df: Spark DataFrame with columns to convert.
        ignore_cols: Ignore columns specified in this list during conversion.

    Returns:
        Spark DataFrame with NUMBER columns converted to DECIMAL(10,0).
    """
    for field in df.schema.fields:
        if (
            isinstance(field.dataType, DecimalType)
            and field.dataType.precision == 38
            and field.dataType.scale == 10
            and field.name not in ignore_cols
        ):
            df = df.withColumn(
                field.name, df[field.name].cast(DecimalType(10, 0))
            )
    return df


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


def dict_to_schema(d: dict) -> str:
    """
    Converts a dictionary to a string in the format 'k1 v1, k2 v2'.

    Args:
        d: Dictionary to convert.

    Returns:
        str: String representation of the dictionary.
    """
    return ", ".join(f"{k} {v}" for k, v in d.items())


def flatten_schema_dicts(
    global_overrides: list[dict] | None, table_overrides: list[dict] | None
) -> dict:
    """
    Flattens a list of dictionaries into a single dictionary.

    Args:
        d: List of dictionaries to flatten.

    Returns:
        dict: A single dictionary containing all key-value pairs from the input
        dictionaries.
    """
    glb = {k: v for item in (global_overrides or []) for k, v in item.items()}
    tab = {k: v for item in (table_overrides or []) for k, v in item.items()}
    glb.update(tab)
    return glb


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
