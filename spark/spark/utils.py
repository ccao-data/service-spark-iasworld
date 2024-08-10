from pathlib import Path
import csv


def read_predicates_from_csv(path: Path) -> list[str]:
    predicates = []
    with open(path.resolve().as_posix(), mode="r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        for row in csv_reader:
            start, end, _ = row
            predicate = f"parid >= '{start}' AND parid <= '{end}'"
            predicates.append(predicate)
    return predicates


def strip_table_prefix(table_name: str) -> str:
    if table_name.startswith("iasworld"):
        return table_name.split(".", 1)[1]
    return table_name
