import json
import time
from pathlib import Path

import jwt


def create_jwt_token(gh_app_id: str, gh_pem_path: str) -> str:
    """
    Generates a JSON web token for authentication with the GitHub API.

    Args:
        gh_app_id: The GitHub App ID.
        gh_pem_path: The path to the GitHub App's private key file.

    Returns:
        str: The JWT token for authentication.
    """

    with open(gh_pem_path, "rb") as pem_file:
        signing_key = jwt.jwk_from_pem(pem_file.read())

    payload = {
        "iat": int(time.time()),
        "exp": int(time.time()) + 60,
        "iss": gh_app_id,
    }

    jwt_instance = jwt.JWT()
    encoded_jwt = jwt_instance.encode(payload, signing_key, alg="RS256")

    return encoded_jwt


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


def strip_table_prefix(table_name: str) -> str:
    """Removes the 'iasworld.' prefix from a table name."""
    if table_name.startswith("iasworld."):
        return table_name.split(".", 1)[1]
    return table_name
