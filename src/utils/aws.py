import boto3
import logging
import os
import time


class SharedAWSClient:
    def __init__(self, logger: logging.Logger):
        """
        Class to store AWS clients and methods for various AWS actions.

        Attributes:
            logger: Spark session logger that outputs to shared file in the
                same format.
            glue_client: Glue client connection. Instantiated from secrets file.
            s3_client: S3 client connection. Instantiated from secrets file.
            s3_bucket: S3 bucket to upload extracts to.
            s3_prefix: S3 path prefix within S3 bucket. Defaults to "iasworld".
        """
        self.logger = logger
        self.glue_client = boto3.client("glue")
        self.s3_client = boto3.client("s3")
        self.s3_bucket = os.getenv("AWS_S3_BUCKET")
        self.s3_prefix = os.getenv("AWS_S3_PREFIX", "iasworld")

    def run_and_wait_for_crawler(self, crawler_name):
        self.logger.info(f"Starting AWS Glue crawler {crawler_name}")
        self.glue_client.start_crawler(Name=crawler_name)

        # Wait for the crawler to complete
        while True:
            response = self.glue_client.get_crawler(Name=crawler_name)
            state = response["Crawler"]["State"]
            if state == "READY":
                self.logger.info(f"Crawler {crawler_name} has completed.")
                break
            elif state == "RUNNING":
                self.logger.info(f"Crawler {crawler_name} is still running...")
            time.sleep(30)
