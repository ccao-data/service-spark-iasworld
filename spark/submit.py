from spark.session import SharedSparkSession
from spark.job import SparkJob
from spark.utils import read_predicates
from pathlib import Path

years = [x for x in range(2019, 2025)]
predicates = read_predicates(Path("/tmp/spark/predicates.csv"), years)
session = SharedSparkSession(app_name="iasworld")

addn = SparkJob(
    session=session,
    table_name="iasworld.addn",
    taxyr=years,
    cur=["Y", "N", "D"],
    predicates=predicates,
)


addn.run()

# addn_2020 = SparkJob(
#     session=session,
#     table_name="iasworld.addn",
#     taxyr=[2020],
#     cur=["Y"],
#     predicates=predicates,
# )
#
# addn_2020.run()

session.spark.stop()
