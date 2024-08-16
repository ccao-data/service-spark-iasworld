import logging
import os
import time
from datetime import datetime

import boto3


class AWSClient:
    def __init__(self, logger: logging.Logger):
        """
        Class to store AWS clients and methods for various AWS actions. Clients
        are instantiated from AWS credentials passed via Compose secrets.

        Attributes:
            logger: Spark session logger that outputs to shared file in the
                same format.
            logs_client: Glue client connection for running crawlers.
            glue_client: CloudWatch logs connection for uploading logs.
            s3_client: S3 client connection for uploading extracted files.
            s3_bucket: S3 bucket to upload extracts to.
            s3_prefix: S3 path prefix within S3 bucket. Defaults to "iasworld".
        """
        self.logger = logger
        self.logs_client = boto3.client("logs")
        self.glue_client = boto3.client("glue")
        self.s3_client = boto3.client("s3")
        self.s3_bucket = os.getenv("AWS_S3_BUCKET")
        self.s3_prefix = os.getenv("AWS_S3_PREFIX", "iasworld")

    def run_and_wait_for_crawler(self, crawler_name) -> None:
        initial_response = self.glue_client.get_crawler(Name=crawler_name)
        if initial_response["Crawler"]["State"] == "READY":  # type: ignore
            self.logger.info(f"Starting AWS Glue crawler {crawler_name}")
            self.glue_client.start_crawler(Name=crawler_name)
        else:
            self.logger.warn(
                f"AWS Glue crawler {crawler_name} is already running"
            )
            return

        # Wait for the crawler to complete before triggering dbt tests
        time_elapsed = 0
        time_increment = 30
        while True:
            response = self.glue_client.get_crawler(Name=crawler_name)
            state = response["Crawler"]["State"]  # type: ignore
            if state in ["READY", "STOPPING"]:
                self.logger.info(f"Crawler {crawler_name} has finished")
                break
            elif state == "RUNNING":
                self.logger.info(
                    (
                        f"Crawler {crawler_name} is running: "
                        f"{time_elapsed}s elapsed"
                    )
                )
            time.sleep(time_increment)
            time_elapsed += time_increment

    def upload_logs_to_cloudwatch(
        self, log_group_name: str, log_stream_name: str, log_file_path: str
    ) -> None:
        """
        Uploads a log file to a specified CloudWatch log group.

        Args:
            log_group_name: The name of the CloudWatch log group.
            log_stream_name: The name of the CloudWatch log stream to write to.
            log_file_path: The path to the log file to upload.
        """
        try:
            with open(log_file_path, "r") as log_file:
                log_events = []
                for line in log_file:
                    timestamp_str, message = line.split(" ", 1)
                    timestamp = int(
                        (
                            datetime.strptime(
                                timestamp_str, "%Y-%m-%d_%H:%M:%S.%f"
                            ).timestamp()
                            * 1000
                        )
                    )
                    log_events.append(
                        {
                            "timestamp": timestamp,
                            "message": message.strip(),
                        }
                    )

            # CloudWatch doesn't allow colon in stream names, so use a dash
            log_stream_name_fmt = log_stream_name.replace(":", "-")
            try:
                self.logs_client.create_log_stream(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name_fmt,
                )
            except self.logs_client.exceptions.ResourceAlreadyExistsException:
                pass

            self.logs_client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name_fmt,
                logEvents=log_events,
            )
            print("Successfully uploaded log file to CloudWatch")

        except Exception as e:
            print(f"Failed to upload log file to CloudWatch: {e}")
