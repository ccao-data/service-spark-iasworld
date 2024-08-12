import csv
import itertools
import json
from argparse import Namespace
from pathlib import Path


def construct_predicates(
    csv_lines: list[list[str]], taxyr: int | list[int] | None = None
) -> list[str]:
    """
    Constructs SQL predicates based on the provided CSV lines and tax year(s).

    Args:
        csv_lines: A list of start and end PARIDs used to divide a table. The
                   third value is the rough number of PARIDs in the batch.
        taxyr: A single tax year, a list of tax years, or None. If provided,
               will be appended to the PARID predicates. Defaults to None.

    Returns:
        A list of SQL predicate strings used to divide a table into chunks.
    """
    taxyr_list = [taxyr] if isinstance(taxyr, int) else taxyr
    if taxyr_list:
        predicates = [
            f"parid >= '{start}' AND parid <= '{end}' AND taxyr = {year}"
            for (start, end, _), year in itertools.product(
                csv_lines, taxyr_list
            )
        ]
    else:
        predicates = [
            f"parid >= '{start}' AND parid <= '{end}'"
            for start, end, _ in csv_lines
        ]

    return predicates


def read_predicates(path: str) -> list[list[str]]:
    """
    Read a CSV file containing PARID start and end boundaries.

    Args:
        path: String path to the CSV file, relative to the Docker container.

    Returns:
        A list of start and end PARIDs used to divide a table. The third value
        is the rough number of PARIDs in the batch.
    """
    pred_path = Path(path)
    with open(pred_path.resolve().as_posix(), mode="r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        csv_lines = [row for row in csv_reader]

    return csv_lines


def strip_table_prefix(table_name: str) -> str:
    """Removes the 'iasworld.' prefix from a table name."""
    if table_name.startswith("iasworld."):
        return table_name.split(".", 1)[1]
    return table_name


def load_job_config(args: Namespace) -> dict:
    """
    Loads job configuration(s) from a JSON file or a JSON string.

    Args:
        args: Namespace object containing the CLI arguments. It should have
              either `json_file` or `json_string` value, but not both.

    Raises:
        ValueError: If both `json_file` and `json_string` are provided, or if
                    neither is provided.

    Returns:
        dict: The job configuration(s) loaded from the JSON file or string.
    """
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
    return job_config
