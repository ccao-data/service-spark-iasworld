from spark.app import SparkApp
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


predicates = read_predicates_from_csv(Path("/tmp/spark/predicates.csv"))

addn = SparkApp(
    app_name="addn",
    table_name="iasworld.addn",
    taxyr=["2019"],
    cur=["Y"],
    predicates=predicates,
)


addn.run()

addn_2020 = SparkApp(
    app_name="addn_2020",
    table_name="iasworld.addn",
    taxyr=["2020"],
    cur=["Y"],
    predicates=predicates,
)

addn_2020.run()
