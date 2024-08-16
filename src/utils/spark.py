import os
import shutil
import time
from datetime import timedelta
from pathlib import Path

from pyarrow import dataset as ds
from pyspark.sql import SparkSession

from utils.aws import AWSClient
from utils.helpers import create_python_logger, strip_table_prefix

logger = create_python_logger(__name__)


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

        # Create the Spark session and logging
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        self.create_spark_logger()

    def create_spark_logger(
        self, log_file_path: str = "/tmp/logs/spark.log"
    ) -> None:
        """
        Extract the logger from the JVM used by Spark, then modify it to write
        to the same log file used by the Python logger.
        """
        # Get Spark logger. See https://stackoverflow.com/a/72740559
        log4j = self.spark.sparkContext._jvm.org.apache.log4j
        spark_logger = log4j.LogManager.getLogger("org.apache.spark")

        # For some reason it's necessary to set the log pattern, even though
        # the format string replicates the default format used by Spark, with
        # the addition of milliseconds
        layout = log4j.PatternLayout()
        layout.setConversionPattern(
            "%d{yyyy-MM-dd_HH:mm:ss.SSS} %p %c{1}: %m%n"
        )

        # Create a file appender to write to a session log file
        appender = log4j.FileAppender()
        appender.setAppend(True)
        appender.setLayout(layout)
        appender.setFile(log_file_path)
        appender.activateOptions()

        spark_logger.removeAllAppenders()
        spark_logger.addAppender(appender)
        return spark_logger


class SparkJob:
    """
    Class to manage Spark jobs for reading, repartitioning, and uploading data.
    Each job corresponds to a single iasWorld table.

    Attributes:
        session: The shared Spark session containing the Spark connection and
            database credentials.
        table_name: The name of the iasWorld table to read from. Should be
            prefixed with 'iasworld.'.
        taxyr: The tax year(s) to filter and partition by.
        cur: The cur value(s) to filter and partition by.
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
        taxyr: list[int] | None,
        cur: list[str] | None,
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

    def get_description(self) -> str:
        """
        Returns a formatted string describing the job, visible in the Spark UI.
        """
        desc = [f"{self.table_name}"]
        if self.taxyr:
            min_year, max_year = self.taxyr[0], self.taxyr[-1]
            desc.append(f"taxyr=[{min_year}, {max_year}]")
        if self.cur:
            desc.append(f"cur=[{', '.join(self.cur)}]")

        return ", ".join(desc)

    def get_filter(self) -> str | None:
        """
        Translates the `taxyr` and `cur` values into SQL used to filter/limit
        the values read from the table.
        """
        filter = []
        if self.taxyr:
            filter.append(f"taxyr IN ({', '.join(map(str, self.taxyr))})")
        if self.cur:
            quoted_cur = [f"'{x}'" for x in self.cur]
            filter.append(f"cur IN ({', '.join(quoted_cur)})")

        filter_join = " AND ".join(filter)

        return filter_join if filter != [] else None

    def get_partitions(self) -> list[str]:
        """
        Sets the partitions for output Parquet files. Partitions are always
        based on the `taxyr` and `cur`, provided they are set.
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
        partitioned by the number of predicates (96 by default).
        """

        time_start = time.time()
        description = self.get_description()
        filter = self.get_filter()
        partitions = self.get_partitions()

        # Create a block of log messages at the start of each job
        logger.info(
            (
                f"Table {self.table_name}: starting JDBC read job",
                "with the following settings",
            )
        )
        logger.info(f"Table {self.table_name} description: {description}")
        if partitions:
            partitions_join = ", ".join(partitions)
            logger.info(
                f"Table {self.table_name} partitions: {partitions_join}"
            )
        if filter:
            filter_join = " AND ".join(filter)
            logger.info(f"Table {self.table_name} filter: {filter_join}")

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

        # Set a nice pretty description in the Spark UI to see which tables
        # are processing
        self.session.spark.sparkContext.setJobDescription(description)

        (
            df.write.mode("overwrite")
            .option("compression", self.session.initial_compression)
            .partitionBy(partitions)
            .parquet(self.initial_dir)
        )

        time_end = time.time()
        time_duration = str(timedelta(seconds=(time_end - time_start)))
        logger.info(f"Table {self.table_name} extracted in {time_duration}")

    def repartition(self) -> None:
        """
        After the initial read, there will be many small Parquet files. This
        method uses PyArrow to repartition the data into a single file per
        Hive partition. We could do this with Spark but it's much slower. The
        goal is to go from this:

            [52K] /tmp/target/initial/addn/taxyr=2020/cur=Y/file1.parquet
            [56K] /tmp/target/initial/addn/taxyr=2020/cur=Y/file2.parquet
            [58K] /tmp/target/initial/addn/taxyr=2020/cur=Y/file3.parquet

        To this:

            [140K] /tmp/target/final/addn/taxyr=2020/cur=Y/part-0.zstd.parquet
        """

        time_start = time.time()
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
            partitioning=self.get_partitions(),
            partitioning_flavor="hive",
            existing_data_behavior="delete_matching",
            file_options=file_options,
            max_rows_per_file=5 * 10**6,
        )

        time_end = time.time()
        time_duration = str(timedelta(seconds=(time_end - time_start)))
        logger.info(
            f"Table {self.table_name} repartitioned in {time_duration}"
        )

    def upload(self, aws: AWSClient) -> list[str]:
        """
        Upload the final partitioned Parquet files to S3. This clears the
        remote S3 equivalent of each local partition prior to upload in order
        to prevent orphan files. It also clears the local directory for the
        table on completion.

        Args:
            aws: AWS client class container S3 connection/location details.

        Returns:
            list[str]: List of previously unseen files uploaded to S3.
        """

        time_start = time.time()
        table_dir = Path(self.final_dir)
        s3_root_prefix = Path(aws.s3_prefix)
        s3_table_prefix = s3_root_prefix / strip_table_prefix(self.table_name)

        # List all files and directories in the local table output directory
        # Example table_files: { "taxyr=2020/part-0.parquet" }
        # Example table_subdirs: { "taxyr=2020/" }
        table_files = set()
        table_subdirs = set()
        for file in table_dir.rglob("*.parquet"):
            if file.is_file():
                file_key = file.relative_to(table_dir)
                dir_key = file.relative_to(table_dir).parent
                table_files.add(file_key)
                table_subdirs.add(dir_key)

        # For each subdirectory in the local output, purge the equivalent
        # subdirectory in S3 of any files that won't be replaced by new uploads.
        # This is to prevent stale files with different S3 keys from hanging
        # around and polluting our data in Athena
        paginator = aws.s3_client.get_paginator("list_objects_v2")
        s3_files = set()
        for subdir in table_subdirs:
            for page in paginator.paginate(
                Bucket=aws.s3_bucket,
                Prefix=(s3_table_prefix / subdir).as_posix(),
            ):
                for obj in page.get("Contents", []):
                    s3_key = Path(obj["Key"]).relative_to(s3_table_prefix)
                    s3_files.add(s3_key)

        s3_files_to_delete = s3_files - table_files
        if s3_files_to_delete:
            delete_objects = [
                {"Key": f"{s3_table_prefix.as_posix()}/{key.as_posix()}"}
                for key in s3_files_to_delete
            ]
            aws.s3_client.delete_objects(
                Bucket=aws.s3_bucket,
                Delete={"Objects": delete_objects},
            )
            logger.info(
                (
                    f"Table {self.table_name} deleting files: "
                    f"{', '.join(map(lambda p: p.as_posix(), s3_files_to_delete))}"
                )
            )

        # List all files in the local table directory to S3
        logger.info(
            f"Table {self.table_name} uploading {len(table_files)} files"
        )
        for file in table_files:
            local_file = table_dir / file
            s3_key = s3_table_prefix / file
            aws.s3_client.upload_file(
                local_file.resolve().as_posix(),
                aws.s3_bucket,
                s3_key.as_posix(),
            )

        # Purge the local output directories once all files have uploaded. This
        # also cleans up the metadata, .crc, and _SUCCESS files from Spark
        shutil.rmtree(self.initial_dir)
        shutil.rmtree(self.final_dir)

        time_end = time.time()
        time_duration = str(timedelta(seconds=(time_end - time_start)))
        logger.info(f"Table {self.table_name} uploaded in {time_duration}")

        new_local_files = [f.as_posix() for f in list(table_files - s3_files)]
        return new_local_files
