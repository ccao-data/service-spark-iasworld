from pyspark.sql import SparkSession
import os


class SharedSparkSession:
    def __init__(self, app_name: str):
        self.app_name = app_name

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
