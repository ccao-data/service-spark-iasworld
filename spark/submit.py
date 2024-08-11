from spark.session import SharedSparkSession
from spark.job import SparkJob
from spark.utils import read_predicates
from pathlib import Path

years = [x for x in range(2023, 2024)]
predicates = read_predicates(Path("/tmp/spark/predicates.csv"))
session = SharedSparkSession(app_name="iasworld")

addn = SparkJob(
    session=session,
    table_name="iasworld.addn",
    taxyr=2022,
    cur=["Y", "N", "D"],
    predicates=predicates,
)


addn.run()

addn_2020 = SparkJob(
    session=session,
    table_name="iasworld.addn",
    taxyr=2023,
    cur=["Y"],
    predicates=predicates,
)

addn_2020.run()

session.spark.stop()

# TODO: This becomes "validate and submit job"
# Separate script used to submit all jobs, wait for spark to finish, then
# use pyarrow to repartition and boto to upload
