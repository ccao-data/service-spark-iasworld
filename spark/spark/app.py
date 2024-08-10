from pyspark.sql import SparkSession
from pathlib import Path
import os


class SparkApp:
    def __init__(
        self,
        app_name: str,
        table_name: str,
        taxyr: str | list[str] | None,
        cur: str | list[str] | None,
        predicates: list[str],
    ) -> None:
        self.app_name = app_name
        self.table_name = table_name
        self.taxyr = taxyr
        self.cur = cur
        self.predicates = predicates

        # Both of these must be set to avoid situations where we create
        # partitions by taxyr but not cur, and visa-versa
        if (self.taxyr is None) != (self.cur is None):
            raise ValueError(
                "Both 'taxyr' and 'cur' must be set if one is set."
            )

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
        with open("/run/secrets/IPTS_PASSWORD", "r") as file:
            self.ipts_password = file.read().strip()

        # Static arguments/params for the Spark connection. The driver path
        # points to a mounted docker volume
        self.driver_path = "/jdbc/ojdbc8.jar"
        self.fetch_size = "10000"
        self.compression = "snappy"

        self.spark = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.jars", self.driver_path)
            .config("spark.driver.extraClassPath", self.driver_path)
            .config("spark.executor.extraClassPath", self.driver_path)
            # Driver mem can be low as it's only used for the JDBC connection
            .config("spark.driver.memory", "2g")
            # Total mem available to the worker, across all jobs
            .config("spark.executor.memory", "96g")
            .getOrCreate()
        )

    def get_target_path(self) -> str:
        target_path = Path("/tmp/target")
        table_path = target_path / self.table_name
        table_path = table_path.resolve().as_posix()
        return table_path

    def get_filter(self) -> str:
        if self.taxyr is None:
            return ""
        if isinstance(self.taxyr, list):
            return f"taxyr IN ({', '.join(self.taxyr)})"
        return f"taxyr = {self.taxyr}"

    def get_partition(self) -> list[str]:
        return ["taxyr", "cur"] if self.taxyr is not None else []

    def run(self) -> None:
        filter = self.get_filter()
        partitions = self.get_partition()
        target_path = self.get_target_path()

        # Must use the JDBC read method here since the normal spark.read()
        # doesn't accept predicates https://stackoverflow.com/a/48680140
        df = self.spark.read.jdbc(
            url=self.database_url,
            table=self.table_name,
            predicates=self.predicates,
            properties={
                "user": self.ipts_username,
                "password": self.ipts_password,
                "fetchsize": self.fetch_size,
            },
        )

        (
            df.filter(filter)
            .write.option("compression", self.compression)
            .partitionBy(partitions)
            .parquet(target_path)
        )
