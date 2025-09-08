import shutil
import tempfile
from polars_utils import utils
import unittest
import polars as pl
from pathlib import Path
import logging
import os
from datetime import datetime
from glob import glob
from moto.core import DEFAULT_ACCOUNT_ID, set_initial_no_auth_action_count
from moto import mock_aws, sns
import boto3
from botocore.exceptions import ClientError

from polars_utils.utils import write_to_parquet, send_sns_notification


class TestUtils(unittest.TestCase):
    logger = logging.getLogger(__name__)

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_write_parquet_does_nothing_for_empty_dataframe(self):
        df: pl.DataFrame = pl.DataFrame({})
        destination: str = os.path.join(self.temp_dir, "test.parquet")
        with self.assertLogs(self.logger) as cm:
            utils.write_to_parquet(df, destination, self.logger, append=False)
        self.assertFalse(Path(destination).exists())
        self.assertTrue(
            "The provided dataframe was empty. No data was written." in cm.output[0]
        )

    def test_write_parquet_writes_simple_dataframe(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        destination: str = os.path.join(self.temp_dir, "test.parquet")
        with self.assertLogs(self.logger) as cm:
            utils.write_to_parquet(df, destination, self.logger, append=False)
        self.assertTrue(Path(destination).exists())
        self.assertTrue(f"Parquet written to {destination}" in cm.output[0])

    def test_write_parquet_writes_with_append(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 1], "b": [4, 5, 6]})
        destination: str = self.temp_dir + "/"
        write_to_parquet(df, destination, self.logger)
        write_to_parquet(df, destination, self.logger)
        self.assertEqual(len(glob(destination + "/**", recursive=True)), 3)

    def test_write_parquet_writes_with_overwrite(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 1], "b": [4, 5, 6]})
        destination: str = os.path.join(self.temp_dir, "test.parquet")
        write_to_parquet(df, destination, self.logger, append=False)
        write_to_parquet(df, destination, self.logger, append=False)

        files = glob(os.path.join(self.temp_dir, "*.parquet"))
        self.assertEqual(len(files), 1)

    def test_generate_s3_datasets_dir_date_path_changes_version_when_version_number_is_passed(
        self,
    ):
        dec_first_21 = datetime(2021, 12, 1)
        version_number = "2.0.0"
        dir_path = utils.generate_s3_datasets_dir_date_path(
            "s3://sfc-main-datasets",
            "test_domain",
            "test_dateset",
            dec_first_21,
            version_number,
        )
        self.assertEqual(
            dir_path,
            "s3://sfc-main-datasets/domain=test_domain/dataset=test_dateset/version=2.0.0/year=2021/month=12/day=01/import_date=20211201/",
        )

    def test_generate_s3_datasets_dir_date_path_uses_version_one_when_no_version_number_is_passed(
        self,
    ):
        dec_first_21 = datetime(2021, 12, 1)
        dir_path = utils.generate_s3_datasets_dir_date_path(
            "s3://sfc-main-datasets", "test_domain", "test_dateset", dec_first_21
        )
        self.assertEqual(
            dir_path,
            "s3://sfc-main-datasets/domain=test_domain/dataset=test_dateset/version=1.0.0/year=2021/month=12/day=01/import_date=20211201/",
        )

    @mock_aws
    def test_send_sns_notification_sends(self):
        client = boto3.client("sns", region_name="eu-west-2")
        topic = client.create_topic(Name="test-topic")

        send_sns_notification(
            topic_arn=topic["TopicArn"],
            subject="Test Subject",
            message="Test Message",
        )

        sns_backend = sns.sns_backends[DEFAULT_ACCOUNT_ID]["eu-west-2"]
        sent_notifications = sns_backend.topics[topic["TopicArn"]].sent_notifications

        assert len(sent_notifications) == 1
        notification = sent_notifications[0]
        assert notification[2] == "Test Subject"
        assert notification[1] == "Test Message"

    @mock_aws
    def test_send_sns_notification_raises_clienterror_and_logs_with_wrong_arn(self):
        client = boto3.client("sns", region_name="eu-west-2")
        topic = client.create_topic(Name="test-topic")
        with self.assertLogs(level="ERROR") as cm:
            with self.assertRaises(ClientError):
                send_sns_notification(
                    topic_arn="silly_arn",
                    subject="Test Subject",
                    message="Test Message",
                )
        self.assertIn(
            "There was an error writing to SNS - check your IAM permissions and that you have the right topic ARN",
            cm.output[1],
        )

    @mock_aws
    @set_initial_no_auth_action_count(1)
    def test_send_sns_notification_raises_clienterror_and_logs_with_no_permissions(
        self,
    ):
        client = boto3.client("sns", region_name="eu-west-2")
        topic = client.create_topic(Name="test-topic")
        with self.assertLogs(level="ERROR") as cm:
            with self.assertRaises(ClientError):
                send_sns_notification(
                    topic_arn=topic["TopicArn"],
                    subject="Test Subject",
                    message="Test Message",
                )
        self.assertIn(
            "There was an error writing to SNS - check your IAM permissions and that you have the right topic ARN",
            cm.output[1],
        )
