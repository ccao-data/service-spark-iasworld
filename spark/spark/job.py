import gc
from pathlib import Path
from pyarrow import dataset as ds
from .session import SharedSparkSession
from .utils import strip_table_prefix


class SparkJob:
    def __init__(
        self,
        session: SharedSparkSession,
        table_name: str,
        taxyr: int | list[int] | None,
        cur: str | list[str] | None,
        predicates: list[str],
        initial_dir: Path = Path("/tmp/target/initial"),
        final_dir: Path = Path("/tmp/target/final"),
    ) -> None:
        self.session = session
        self.table_name = table_name
        self.taxyr = taxyr
        self.cur = cur
        self.predicates = predicates
        self.initial_dir = (
            (initial_dir / strip_table_prefix(self.table_name))
            .resolve()
            .as_posix()
        )
        self.final_dir = (
            (final_dir / strip_table_prefix(self.table_name))
            .resolve()
            .as_posix()
        )

        # Both of these must be set to avoid situations where we create
        # partitions by taxyr but not cur, and visa-versa
        if (self.taxyr is None) != (self.cur is None):
            raise ValueError(
                "Both 'taxyr' and 'cur' must be set if one is set."
            )

    def get_filter(self) -> str:
        if self.taxyr is None:
            return ""

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

        (
            df.filter(filter)
            .write.mode("append")
            .option("compression", self.session.compression)
            .partitionBy(partitions)
            .parquet(self.initial_dir)
        )

        df.unpersist(blocking=True)
        gc.collect()

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

    def run(self) -> None:
        self.read()
        self.repartition()
