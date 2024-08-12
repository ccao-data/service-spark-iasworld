import csv
import json
from pathlib import Path


def construct_predicates(pred_path: str, pred_type: str) -> list[str]:
    """
    Constructs SQL BETWEEN predicates based on a provided CSV file.

    Args:
        pred_path: String path to a CSV file within the `config/`
            directory. The CSV file should define the column, start value, and
            end value used to construct a SQL BETWEEN expression. Each line
            creates its own expression equivalent to one chunk of the table
            during reading.
        pred_type: Data type of the predicate column, either "string" or
           "numeric". The "string" type is quoted in the generated SQL BETWEEN,
           while the "numeric" type is not.

    Returns:
        A list of SQL predicate strings used to divide a table into chunks
        during JDBC read jobs.
    """
    if pred_type not in {"string", "numeric"}:
        raise ValueError("pred_type must be either 'string' or 'numeric'")

    full_path = "/tmp/config" / Path(pred_path)
    with open(full_path.resolve().as_posix(), mode="r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row
        csv_lines = [row for row in csv_reader]

    q = "'" if pred_type == "string" else ""
    predicates = [
        f"({column} BETWEEN {q}{start}{q} AND {q}{end}{q})"
        for (column, start, end) in csv_lines
    ]

    return predicates


def strip_table_prefix(table_name: str) -> str:
    """Removes the 'iasworld.' prefix from a table name."""
    if table_name.startswith("iasworld."):
        return table_name.split(".", 1)[1]
    return table_name


def load_job_definitions(json_file: str, json_string: str) -> dict:
    """
    Loads job definition(s) from a JSON file or a JSON string.

    Args:
        json_file: String path to
            either `json_file` or `json_string` value, but not both.

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
