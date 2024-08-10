from spark.session import SharedSparkSession
from spark.job import SparkJob
from spark.utils import read_predicates_from_csv
from pathlib import Path

predicates = read_predicates_from_csv(Path("/tmp/spark/predicates.csv"))
session = SharedSparkSession(app_name="iasworld")

addn = SparkJob(
    session=session,
    table_name="iasworld.addn",
    taxyr=[2019],
    cur=["Y"],
    predicates=predicates,
)


addn.run()

addn_2020 = SparkJob(
    session=session,
    table_name="iasworld.addn",
    taxyr=[2020],
    cur=["Y"],
    predicates=predicates,
)

addn_2020.run()

session.spark.stop()
