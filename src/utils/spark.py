from pathlib import Path
from pyarrow import dataset as ds
from pyspark.sql import SparkSession
from utils.helpers import strip_table_prefix
import os


class SharedSparkSession:
    def __init__(
        self, app_name: str, password_file_path: str, driver_file_path: str
    ) -> None:
        self.app_name = app_name
        self.password_file_path = password_file_path
        self.driver_file_path = driver_file_path

        # Vars here are loaded from the .env file, then forwarded to the
        # container in docker-compose.yaml
        self.ipts_hostname = os.getenv("IPTS_HOSTNAME")
        self.ipts_port = os.getenv("IPTS_PORT")
        self.ipts_service_name = os.getenv("IPTS_SERVICE_NAME")
        self.ipts_username = os.getenv("IPTS_USERNAME")
        self.database_url = (
            f"jdbc:oracle:thin:@//{self.ipts_hostname}:"
            f"{self.ipts_port}/"
            f"{self.ipts_service_name}"
        )

        # Load runtime secret using Compose secrets setup. The file address
        # doesn't change, so it's hardcoded here
        with open(self.password_file_path, "r") as file:
            self.ipts_password = file.read().strip()

        # Static arguments/params for the Spark connection. The driver path
        # points to a mounted docker volume
        self.fetch_size = "10000"
        self.compression = "snappy"

        self.spark = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.jars", self.driver_file_path)
            .config("spark.driver.extraClassPath", self.driver_file_path)
            .config("spark.driver.extraClassPath", self.driver_file_path)
            # Driver mem can be low as it's only used for the JDBC connection
            .config("spark.driver.memory", "2g")
            # Total mem available to the worker, across all jobs
            .config("spark.executor.memory", "96g")
            .getOrCreate()
        )


class SparkJob:
    def __init__(
        self,
        session: SharedSparkSession,
        table_name: str,
        taxyr: int | list[int] | None,
        cur: str | list[str] | None,
        predicates: list[str] | None,
        initial_dir: str,
        final_dir: str,
    ) -> None:
        self.session = session
        self.table_name = table_name
        self.taxyr = taxyr
        self.cur = cur
        self.predicates = predicates
        self.initial_dir = (
            (Path(initial_dir) / strip_table_prefix(self.table_name))
            .resolve()
            .as_posix()
        )
        self.final_dir = (
            (Path(final_dir) / strip_table_prefix(self.table_name))
            .resolve()
            .as_posix()
        )

        # Both of these must be set to avoid situations where we create
        # partitions by taxyr but not cur, and visa-versa
        if (self.taxyr is None) != (self.cur is None):
            raise ValueError(
                (
                    f"Error for table {self.table_name}: "
                    "both 'taxyr' and 'cur' must be set if one is set."
                )
            )

    def get_filter(self) -> str | None:
        if self.taxyr is None:
            return None

        if isinstance(self.taxyr, list):
            filter = f"taxyr IN ({', '.join(map(str, self.taxyr))})"
        else:
            filter = f"taxyr = {self.taxyr}"

        if isinstance(self.cur, list):
            quoted_cur = [f"'{x}'" for x in self.cur]
            filter += f" AND cur IN ({', '.join(quoted_cur)})"
        else:
            filter += f" AND cur = '{self.cur}'"
        return filter

    def get_partition(self) -> list[str]:
        return ["taxyr", "cur"] if self.taxyr is not None else []

    """
    Perform the initial file write to disk. This will be partitioned by the
    number of values passed via predicates (by default 96)
    """

    def read(self) -> None:
        filter = self.get_filter()
        partitions = self.get_partition()

        # Must use the JDBC read method here since the normal spark.read()
        # doesn't accept predicates https://stackoverflow.com/a/48680140
        df = self.session.spark.read.jdbc(
            url=self.session.database_url,
            table=self.table_name,
            predicates=self.predicates,
            properties={
                "user": self.session.ipts_username,
                "password": self.session.ipts_password,
                "fetchsize": self.session.fetch_size,
            },
        )

        # Only apply the filtering step if limiting values were actually passed
        if filter:
            df = df.filter(filter)

        (
            df.write.mode("overwrite")
            .option("compression", self.session.compression)
            .partitionBy(partitions)
            .parquet(self.initial_dir)
        )

    """
    Rewrite the read data from Spark into a single file per Hive
    partition. It's MUCH faster to do this via pyarrow than via Spark
    itself, even within the Spark job.
    """

    def repartition(self) -> None:
        dataset = ds.dataset(
            source=self.initial_dir,
            format="parquet",
            partitioning="hive",
        )
        file_options = ds.ParquetFileFormat().make_write_options(
            compression="zstd"
        )
        # Very important to set the delete_matching option in order
        # to remove the old partitions when writing new ones
        ds.write_dataset(
            data=dataset,
            base_dir=self.final_dir,
            format="parquet",
            partitioning=self.get_partition(),
            partitioning_flavor="hive",
            existing_data_behavior="delete_matching",
            file_options=file_options,
            max_rows_per_file=5 * 10**6,
        )
