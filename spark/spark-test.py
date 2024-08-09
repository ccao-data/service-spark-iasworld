from pyarrow import dataset as ds
from pyspark.sql import SparkSession
import os
import csv

IPTS_HOSTNAME = os.getenv("IPTS_HOSTNAME")
IPTS_PORT = os.getenv("IPTS_PORT")
IPTS_SERVICE_NAME = os.getenv("IPTS_SERVICE_NAME")
IPTS_USERNAME = os.getenv("IPTS_USERNAME")

with open("/run/secrets/IPTS_PASSWORD", "r") as file:
    IPTS_PASSWORD = file.read().strip()

database_url = (
    f"jdbc:oracle:thin:@//{IPTS_HOSTNAME}:{IPTS_PORT}/{IPTS_SERVICE_NAME}"
)
driver_path = "/jdbc/ojdbc8.jar"

spark = (
    SparkSession.builder.appName("pyspark jdbc query")
    .config("spark.jars", driver_path)
    .config("spark.driver.extraClassPath", driver_path)
    .config("spark.executor.extraClassPath", driver_path)
    .config("spark.driver.memory", "2g")  # Set driver memory
    .config("spark.executor.memory", "96g")  # Set executor memory
    .getOrCreate()
)


def read_predicates_from_csv(file_path):
    predicates = []
    with open(file_path, mode="r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        for row in csv_reader:
            start, end, _ = row
            predicate = f"parid >= '{start}' AND parid <= '{end}'"
            predicates.append(predicate)
    return predicates


# Example usage
file_path = "predicates.csv"
predicates = read_predicates_from_csv(file_path)
print(predicates)

# df = (
#     spark.read.format("jdbc")
#     .option("url", database_url)
#     .option("dbtable", "iasworld.asmt_all")
#     .option("user", IPTS_USERNAME)
#     .option("password", IPTS_PASSWORD)
#     .option("predicates", predicates)
#     .option("fetchsize", "10000")
#     .load()
# )

df = spark.read.jdbc(
    url=database_url,
    table="iasworld.asmt_all",
    properties={
        "user": IPTS_USERNAME,
        "password": IPTS_PASSWORD,
        "fetchsize": "10000",
    },
    predicates=predicates,
)

df.filter("taxyr == 2023").write.option("compression", "zstd").partitionBy(
    "taxyr", "cur"
).parquet("/tmp/target/test")

data = ds.dataset("/tmp/target/test", format="parquet", partitioning="hive")

ds.write_dataset(
    data,
    "/tmp/target/test2",
    format="parquet",
    partitioning=["taxyr", "cur"],
    partitioning_flavor="hive",
    min_rows_per_group=25000,
    max_rows_per_file=5 * 10**6,
)
