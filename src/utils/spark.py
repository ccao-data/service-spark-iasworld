import os
from pathlib import Path

from pyarrow import dataset as ds
from pyspark.sql import SparkSession

from utils.helpers import strip_table_prefix


class SharedSparkSession:
    """
    Class to manage a shared Spark session connected to iasWorld via JDBC.
    Contains all the credentials and settings needed to run each Spark job.
    Only one of these should be created per batch of jobs.

    Attributes:
        app_name: The name of the Spark application. Used in the UI and can
            be referenced to poll session status.
        password_file_path: The path to the file containing the password. This
            is passed via Compose secrets.
        ipts_hostname: The hostname for the iasWorld database.
        ipts_port: The port for the iasWorld database.
        ipts_service_name: The service name for the iasWorld database.
        ipts_username: The username for the iasWorld database.
        database_url: The JDBC URL for the database connection. Constructed
            from the above attributes.
        ipts_password: The password for the database, read from file.
        fetch_size: The fetch size for the database queries. This is a tuning
            parameter for query speed. ~10,000 seems to work best.
        initial_compression: The compression type for the initial Parquet files
            written via JDBC extract. Defaults to snappy.
        final_compression: The compression type final repartitioned Parquet
            files. Defaults to ztd.
        spark: The Spark session object.
    """

    def __init__(self, app_name: str, password_file_path: str) -> None:
        self.app_name = app_name
        self.password_file_path = password_file_path

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

        # Load runtime secret using Compose secrets setup
        with open(self.password_file_path, "r") as file:
            self.ipts_password = file.read().strip()

        # Static arguments/params applied to jobs using this session
        self.fetch_size = "10000"
        self.initial_compression = "snappy"
        self.final_compression = "zstd"

        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()


class SparkJob:
    """
    Class to manage Spark jobs for reading, repartitioning, and uploading data.
    Each job corresponds to a single iasWorld table.

    Attributes:
        session: The shared Spark session containing the Spark connection and
            database credentials.
        table_name: The name of the iasWorld table to read from. Should be
            predicated with 'iasworld.'.
        taxyr: The tax year(s) to filter and partition by.
        cur: The cur values to filter and partition by.
        predicates: A list of SQL predicates for chunking JDBC reads.
        initial_dir: The initial directory to write the data to, relative to
            the Docker container.
        final_dir: The final directory to write the repartitioned data to,
            relative to the Docker container.
    """

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

    def get_filter(self) -> str | None:
        """
        Translates the taxyr and cur values into SQL used to filter/limit the
        values read from the table.
        """
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
        """
        Partition values used for Hive-style partitions e.g. the outputs from
        read() will look something like:

            /tmp/target/initial/addn/taxyr=2020/cur=Y/big_file_name1.parquet
            /tmp/target/initial/addn/taxyr=2021/cur=N/big_file_name1.parquet

        Uses `taxyr` or `cur` if either is set, or both if both are set.
        """
        partitions = []
        if self.taxyr:
            partitions.append("taxyr")
        if self.cur:
            partitions.append("cur")
        return partitions

    def read(self) -> None:
        """
        Perform the JDBC read and the initial file write to disk. Files will be
        partitioned by the number of values passed via predicates (by default
        96 per taxyr).
        """
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

        # Only apply the filtering step if limiting values are actually passed
        # because it errors with an empty string or None value
        if filter:
            df = df.filter(filter)

        (
            df.write.mode("overwrite")
            .option("compression", self.session.initial_compression)
            .partitionBy(partitions)
            .parquet(self.initial_dir)
        )

    def repartition(self) -> None:
        """
        After the initial read, there will be many small Parquet files. This
        method uses pyarrow to repartition the data into a single file per
        Hive partition. We could do this with Spark but it's much slower. The
        goal is to go from this:

            [52K] /tmp/target/initial/addn/taxyr=2020/cur=Y/file1.parquet
            [56K] /tmp/target/initial/addn/taxyr=2020/cur=Y/file2.parquet
            [58K] /tmp/target/initial/addn/taxyr=2020/cur=Y/file3.parquet

        To this:

            [140K] /tmp/target/final/addn/taxyr=2020/cur=Y/part-0.zstd.parquet
        """
        dataset = ds.dataset(
            source=self.initial_dir,
            format="parquet",
            partitioning="hive",
        )
        file_options = ds.ParquetFileFormat().make_write_options(
            compression=self.session.final_compression
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
