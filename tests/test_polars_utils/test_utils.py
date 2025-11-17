import argparse
import io
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime
from glob import glob
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import boto3
import polars as pl
import polars.testing as pl_testing
from botocore.exceptions import ClientError
from moto import mock_aws, sns
from moto.core import DEFAULT_ACCOUNT_ID, set_initial_no_auth_action_count

from polars_utils import utils

SRC_PATH = "polars_utils.validation.actions"
PATCH_PATH = "polars_utils.utils"


class TestUtils(unittest.TestCase):

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.types_df = pl.DataFrame(
            {
                "foo": [1, 2, 3],
                "bar": [None, "bak", "baz"],
                "a_list": [
                    [[1, 2], [1], None],
                    [[1, 2], [2], None],
                    [[1, 2], [3], None],
                ],
            }
        ).with_columns(pl.struct(pl.all()).alias("a_struct"))

        self.df = pl.DataFrame(
            [
                ("1-00001", "20240101", "a"),
                ("1-00002", "20240101", "b"),
                ("1-00001", "20240201", "b"),
                ("1-00002", "20240201", "c"),
                ("1-00002", "20240201", None),
            ],
            schema=pl.Schema(
                [
                    ("someId", pl.String),
                    ("my_date", pl.String),
                    ("name", pl.String),
                ]
            ),
            orient="row",
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


@patch(f"{PATCH_PATH}.boto3.client")
class TestScanParquet(TestUtils):
    def test_uses_schema(self, mock_s3_client: Mock):
        # GIVEN
        #   Dataframe written to parquet and schema is directly inferred
        self.df.write_parquet(self.temp_dir / "df.parquet")
        schema = self.df.schema
        #   File mocked as existing
        mock_s3_client.return_value.list_objects_v2.return_value = ["Contents"]

        # WHEN
        return_lf = utils.scan_parquet(source=self.temp_dir, schema=schema)

        # THEN
        #   The returned instance should be a LazyFrame
        self.assertIsInstance(return_lf, pl.LazyFrame)
        #   The returned lf should be equal to the dataframe that was originally written out
        pl_testing.assert_frame_equal(self.df.lazy(), return_lf)

    def test_infers_schema_if_not_provided(self, mock_s3_client: Mock):
        # GIVEN
        #   Dataframe written to parquet but no schema is inferred
        self.df.write_parquet(self.temp_dir / "df.parquet")
        #   File mocked as existing
        mock_s3_client.return_value.list_objects_v2.return_value = ["Contents"]

        # WHEN
        return_lf = utils.scan_parquet(source=self.temp_dir)

        # THEN
        #   The returned instance should be a LazyFrame
        self.assertIsInstance(return_lf, pl.LazyFrame)
        #   The returned lf should be equal to the dataframe that was originally written out
        pl_testing.assert_frame_equal(self.df.lazy(), return_lf)

    def test_selects_columns(self, mock_s3_client: Mock):
        # GIVEN
        #   Dataframe written to parquet but no schema is inferred
        self.df.write_parquet(self.temp_dir / "df.parquet")
        selected_columns = ["someId"]
        #   File mocked as existing
        mock_s3_client.return_value.list_objects_v2.return_value = ["Contents"]

        # WHEN
        return_lf = utils.scan_parquet(
            source=self.temp_dir, selected_columns=selected_columns
        )

        # THEN
        #   The returned instance should be a LazyFrame
        self.assertIsInstance(return_lf, pl.LazyFrame)
        #   The returned lf should be equal to the dataframe that was originally written out
        pl_testing.assert_frame_equal(self.df.select("someId").lazy(), return_lf)


class TestReadParquet(TestUtils):
    def test_read_parquet_keep_all(self):
        # Given
        self.types_df.write_parquet(self.temp_dir / "types.parquet")
        expected = pl.DataFrame(
            {
                "foo": [1, 2, 3],
                "bar": [None, "bak", "baz"],
                "a_list": [
                    [[1, 2], [1], None],
                    [[1, 2], [2], None],
                    [[1, 2], [3], None],
                ],
                "a_struct": [
                    {"foo": 1, "bar": None, "a_list": [[1, 2], [1], None]},
                    {"foo": 2, "bar": "bak", "a_list": [[1, 2], [2], None]},
                    {"foo": 3, "bar": "baz", "a_list": [[1, 2], [3], None]},
                ],
            }
        )
        # When
        result = utils.read_parquet(self.temp_dir)
        # Then
        self.assertTrue(result.equals(expected))

    def test_read_parquet_exclude_complex(self):
        # Given
        self.types_df.write_parquet(self.temp_dir / "test.parquet")
        expected = pl.DataFrame(
            {
                "foo": [1, 2, 3],
                "bar": [None, "bak", "baz"],
            }
        )
        # When
        result = utils.read_parquet(self.temp_dir, exclude_complex_types=True)
        # Then
        self.assertTrue(result.equals(expected))


class TestWriteParquet(TestUtils):
    def test_write_parquet_does_nothing_for_empty_dataframe(self):
        df: pl.DataFrame = pl.DataFrame({})
        # destination: str = os.path.join(self.temp_dir, "test.parquet")
        destination = self.temp_dir / "test.parquet"

        f = io.StringIO()
        with redirect_stdout(f):
            utils.write_to_parquet(df, destination, append=False)
        output = f.getvalue()

        self.assertFalse(destination.exists())
        self.assertTrue(
            "The provided dataframe was empty. No data was written." in output
        )

    def test_write_parquet_writes_simple_dataframe(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        destination = self.temp_dir / "test.parquet"

        f = io.StringIO()
        with redirect_stdout(f):
            utils.write_to_parquet(df, destination, append=False)
        output = f.getvalue()

        self.assertTrue(Path(destination).exists())
        self.assertTrue(f"Parquet written to {destination}" in output)

    def test_write_parquet_writes_with_append(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 1], "b": [4, 5, 6]})
        destination: str = str(self.temp_dir) + "/"
        utils.write_to_parquet(df, destination)
        utils.write_to_parquet(df, destination)
        self.assertEqual(len(glob(destination + "/**", recursive=True)), 3)

    def test_write_parquet_writes_with_overwrite(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 1], "b": [4, 5, 6]})
        destination = self.temp_dir / "test.parquet"
        utils.write_to_parquet(df, destination, append=False)
        utils.write_to_parquet(df, destination, append=False)

        files = glob(os.path.join(self.temp_dir, "*.parquet"))
        self.assertEqual(len(files), 1)


class TestSinkParquet(TestUtils):
    def test_sink_parquet_does_nothing_for_empty_lazyframe(self):
        lazy_df: pl.LazyFrame = pl.DataFrame({}).lazy()
        destination = self.temp_dir / "test.parquet"

        f = io.StringIO()
        with redirect_stdout(f):
            utils.sink_to_parquet(lazy_df, destination, None, append=False)
        output = f.getvalue()

        self.assertFalse(destination.exists())
        self.assertTrue(
            "The provided LazyFrame was empty. No data was written." in output
        )

    def test_sink_parquet_writes_simple_lazyframe(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        lazy_df = df.lazy()
        destination: str = str(self.temp_dir) + "/"
        utils.sink_to_parquet(lazy_df, destination, append=True)
        files = glob(os.path.join(str(destination), "*.parquet"))
        self.assertEqual(len(files), 1)

    def test_sink_parquet_writes_with_append(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 1], "b": [4, 5, 6]})
        lazy_df = df.lazy()
        destination: str = str(self.temp_dir) + "/"
        utils.sink_to_parquet(lazy_df, destination, append=True)
        utils.sink_to_parquet(lazy_df, destination, append=True)
        files = glob(os.path.join(destination, "*.parquet"))
        self.assertEqual(len(files), 2)

    def test_sink_parquet_writes_with_overwrite(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 1], "b": [4, 5, 6]})
        lazy_df = df.lazy()
        destination = self.temp_dir / "test.parquet"
        utils.sink_to_parquet(lazy_df, destination, append=False)
        utils.sink_to_parquet(lazy_df, destination, append=False)

        files = glob(os.path.join(self.temp_dir, "*.parquet"))
        self.assertEqual(len(files), 1)

    def test_sink_parquet_with_partitioning(self):
        df: pl.DataFrame = pl.DataFrame(
            {"a": [1, 2, 1, 2], "b": [4, 5, 6, 7], "part": ["x", "x", "y", "y"]}
        )
        lazy_df = df.lazy()
        destination = self.temp_dir / "partition_test"
        utils.sink_to_parquet(
            lazy_df,
            destination,
            partition_cols=["part"],
            append=False,
        )
        partitions = [d.name for d in destination.iterdir() if d.is_dir()]
        self.assertTrue(set(partitions) == {"part=x", "part=y"})

    def test_day_month_partition_casting(self):
        lazy_df = pl.LazyFrame(
            {
                "id": ["1-001", "1-002"],
                "value": [1, 200],
                "year": [2024, 2025],
                "month": [4, 11],
                "day": [15, 5],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir)

            utils.sink_to_parquet(
                lazy_df,
                output_path,
                partition_cols=["year", "month", "day"],
                append=False,
            )

            written_files = [str(p) for p in output_path.rglob("*.parquet")]

            expected_partition_1 = Path("year=2024", "month=04", "day=15")
            expected_partition_2 = Path("year=2025", "month=11", "day=05")

            self.assertEqual(len(written_files), 2)
            self.assertTrue(any(str(expected_partition_1) in p for p in written_files))
            self.assertTrue(any(str(expected_partition_2) in p for p in written_files))


class TestGenerateS3Dir(TestUtils):
    def test_get_date_partitioned_s3_path_version(self):
        dec_first_21 = datetime(2021, 12, 1)
        version_number = "2.0.0"
        dir_path = utils.generate_s3_dir(
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

    def test_get_date_partitioned_s3_path_default_version(self):
        dec_first_21 = datetime(2021, 12, 1)
        dir_path = utils.generate_s3_dir(
            "s3://sfc-main-datasets", "test_domain", "test_dateset", dec_first_21
        )
        self.assertEqual(
            dir_path,
            "s3://sfc-main-datasets/domain=test_domain/dataset=test_dateset/version=1.0.0/year=2021/month=12/day=01/import_date=20211201/",
        )


class TestGetArgs(TestUtils):
    def test_get_args_has_all(self):
        # Given
        args = (
            ("--arg1", "help"),
            ("--arg2", "help", False),
            ("--arg3", "help", False, "default"),
        )
        with patch.object(
            sys, "argv", ["prog", "--arg1", "value1", "--arg2", "value2"]
        ):
            # When
            parsed = utils.get_args(*args)
            # Then
            self.assertEqual(parsed.arg1, "value1")
            self.assertEqual(parsed.arg2, "value2")
            self.assertEqual(parsed.arg3, "default")

    def test_get_args_missing_required(self):
        # Given
        args = (
            ("--arg1", "help"),
            ("--arg2", "help", True),
        )
        with patch.object(sys, "argv", ["prog", "--arg1", "value1"]):
            # When / Then
            with self.assertRaises(argparse.ArgumentError):
                utils.get_args(*args)

    def test_get_args_missing_optional(self):
        # Given
        args = (
            ("--arg1", "help"),
            ("--arg2", "help", False),
        )
        with patch.object(sys, "argv", ["prog", "--arg1", "value1"]):
            # When
            parsed = utils.get_args(*args)
            # Then
            self.assertEqual(parsed.arg1, "value1")
            self.assertEqual(parsed.arg2, None)

    def test_extra_args_fails(self):
        # Given
        args = (("--arg1", "help"),)
        with patch.object(
            sys, "argv", ["prog", "--arg1", "value1", "--arg2", "value2"]
        ):
            # When / Then
            with self.assertRaises(argparse.ArgumentError):
                utils.get_args(*args)


class TestListS3ParquetImportDates(unittest.TestCase):
    @patch(f"{PATCH_PATH}.boto3.client")
    def test_no_objects(self, mock_boto_client_mock: Mock):
        """Test when the S3 prefix has no import_date folders."""
        # Mock paginator to return empty Contents
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_boto_client_mock.return_value = mock_s3

        result = utils.list_s3_parquet_import_dates(
            "s3://test_bucket/domain=test_domain/dataset=test_dataset/"
        )
        self.assertEqual(result, [])

    @patch(f"{PATCH_PATH}.boto3.client")
    def test_multiple_import_dates_sorted_chronologically(
        self, mock_boto_client_mock: Mock
    ):
        """Test multiple import_date folders are sorted chronologically in list."""
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "domain=test_domain/dataset=test_dataset/year=2025/month=12/day=01/import_date=20251201/file.parquet"
                    },
                    {
                        "Key": "domain=test_domain/dataset=test_dataset/year=2023/month=05/day=01/import_date=20230501/file.parquet"
                    },
                    {
                        "Key": "domain=test_domain/dataset=test_dataset/year=2024/month=01/day=01/import_date=20240101/file.parquet"
                    },
                ]
            }
        ]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_boto_client_mock.return_value = mock_s3

        result = utils.list_s3_parquet_import_dates(
            "s3://test_bucket/domain=test_domain/dataset=test_dataset/"
        )
        # Should be sorted
        self.assertEqual(result, [20230501, 20240101, 20251201])

    @patch(f"{PATCH_PATH}.boto3.client")
    def test_invalid_prefix_format(self, mock_boto_client_mock: Mock):
        """Test that non-import_date prefixes are ignored."""
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "domain=test_domain/dataset=test_dataset/other=123/file.parquet"
                    },
                    {
                        "Key": "domain=test_domain/dataset=test_dataset/year=2023/month=05/day=01/import_date=20230501/file.parquet"
                    },
                ]
            }
        ]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_boto_client_mock.return_value = mock_s3

        result = utils.list_s3_parquet_import_dates(
            "s3://test_bucket/domain=test_domain/dataset=test_dataset/"
        )
        self.assertEqual(result, [20230501])

    @patch(f"{PATCH_PATH}.boto3.client")
    def test_bucket_and_prefix_parsing(self, mock_boto_client_mock: Mock):
        """Ensure s3 URI is parsed correctly into bucket and prefix."""
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_boto_client_mock.return_value = mock_s3

        # Should not raise an error
        result = utils.list_s3_parquet_import_dates("s3://test_bucket/path/to/prefix/")
        self.assertEqual(result, [])


class TestEmptyS3Folder(unittest.TestCase):
    @patch("boto3.client")
    def test_empty_s3_folder_no_objects(self, mock_s3_client):
        # Given
        paginator = mock_s3_client.return_value.get_paginator.return_value
        paginator.paginate.return_value.search.return_value = [None]

        # When
        f = io.StringIO()
        with redirect_stdout(f):
            utils.empty_s3_folder("my-bucket", "some/prefix/")
        output = f.getvalue()

        # Then
        self.assertIn(
            "Skipping emptying folder - no objects matching prefix some/prefix/",
            output,
        )
        mock_s3_client.return_value.delete_objects.assert_not_called()

    @patch("boto3.client")
    def test_empty_s3_folder_deletes_objects(self, mock_s3_client):
        # Given
        paginator = mock_s3_client.return_value.get_paginator.return_value
        paginator.paginate.return_value.search.side_effect = [
            [
                {"Key": "some/prefix/file1.parquet"},
                {"Key": "some/prefix/file2.parquet"},
                {"Key": "some/prefix/file3.parquet"},
            ]
        ]
        # When
        utils.empty_s3_folder("my-bucket", "some/prefix/")
        # Then
        mock_s3_client.return_value.delete_objects.assert_called_once_with(
            Bucket="my-bucket",
            Delete={
                "Objects": [
                    {"Key": "some/prefix/file1.parquet"},
                    {"Key": "some/prefix/file2.parquet"},
                    {"Key": "some/prefix/file3.parquet"},
                ]
            },
        )


class SendSnsNotificationTests(TestUtils):
    @mock_aws
    def test_send_sns_notification_sends(self):
        client = boto3.client("sns", region_name="eu-west-2")
        topic = client.create_topic(Name="test-topic")

        utils.send_sns_notification(
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

        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises(ClientError):
                utils.send_sns_notification(
                    topic_arn="silly_arn",
                    subject="Test Subject",
                    message="Test Message",
                )
        output = f.getvalue()

        self.assertIn(
            "There was an error writing to SNS - check your IAM permissions and that you have the right topic ARN",
            output,
        )

    @mock_aws
    @set_initial_no_auth_action_count(1)
    def test_send_sns_notification_raises_clienterror_and_logs_with_no_permissions(
        self,
    ):
        client = boto3.client("sns", region_name="eu-west-2")
        topic = client.create_topic(Name="test-topic")

        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises(ClientError):
                utils.send_sns_notification(
                    topic_arn=topic["TopicArn"],
                    subject="Test Subject",
                    message="Test Message",
                )
        output = f.getvalue()

        self.assertIn(
            "There was an error writing to SNS - check your IAM permissions and that you have the right topic ARN",
            output,
        )

    def test_parse_args_converts_simple_data_types(self):
        self.assertTrue(utils.parse_arg_by_type("tRue"))
        self.assertFalse(utils.parse_arg_by_type("fAlSe "))
        self.assertEqual(19, utils.parse_arg_by_type("19 "))
        self.assertEqual(19, utils.parse_arg_by_type(" 19"))
        self.assertEqual(23.7, utils.parse_arg_by_type("23.7 "))
        self.assertEqual(
            "Dave is 173.4 cm tall", utils.parse_arg_by_type("Dave is 173.4 cm tall")
        )

    def test_parse_args_converts_complex_types(self):
        self.assertEqual("[1, 2, 3]", utils.parse_arg_by_type("[1, 2, 3]"))
        self.assertEqual("{1:2, 3:4}", utils.parse_arg_by_type("{1:2, 3:4}"))
        self.assertEqual("2025-06-19", utils.parse_arg_by_type("2025-06-19"))


class TestSplitS3Uri(TestUtils):
    def test_split_s3_uri(self):
        s3_uri = "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/"
        bucket_name, key_name = utils.split_s3_uri(s3_uri)

        self.assertEqual(bucket_name, "sfc-data-engineering-raw")
        self.assertEqual(key_name, "domain=ASCWDS/dataset=workplace/")


class TestFilterToMaximumValueInColumn(TestUtils):
    def setUp(self) -> None:
        super().setUp()

        self.lf = pl.LazyFrame(
            {
                "id": ["1", "2", "3"],
                "date_col": [date(2024, 1, 1), date(2024, 1, 1), date(2023, 1, 1)],
                "string_col": ["20220101", "20230101", "20240101"],
            }
        )

    def test_filters_correctly_with_date_column(self):
        returned_lf = utils.filter_to_maximum_value_in_column(self.lf, "date_col")

        expected_lf = pl.LazyFrame(
            {
                "id": ["1", "2"],
                "date_col": [date(2024, 1, 1), date(2024, 1, 1)],
                "string_col": ["20220101", "20230101"],
            }
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_filters_correctly_with_string_column(self):
        returned_lf = utils.filter_to_maximum_value_in_column(self.lf, "string_col")

        expected_lf = pl.LazyFrame(
            {
                "id": ["3"],
                "date_col": [date(2023, 1, 1)],
                "string_col": ["20240101"],
            }
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
