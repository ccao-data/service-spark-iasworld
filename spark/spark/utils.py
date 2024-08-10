from pathlib import Path
import csv
import itertools


def read_predicates(path: Path, taxyr: int | list[int] | None) -> list[str]:
    with open(path.resolve().as_posix(), mode="r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        csv_lines = [row for row in csv_reader]

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


def strip_table_prefix(table_name: str) -> str:
    if table_name.startswith("iasworld"):
        return table_name.split(".", 1)[1]
    return table_name
