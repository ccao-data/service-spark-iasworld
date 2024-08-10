from pathlib import Path
from .session import SharedSparkSession


class SparkJob:
    def __init__(
        self,
        session: SharedSparkSession,
        table_name: str,
        taxyr: str | list[str] | None,
        cur: str | list[str] | None,
        predicates: list[str],
    ) -> None:
        self.session = session
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
            .parquet(target_path)
        )
