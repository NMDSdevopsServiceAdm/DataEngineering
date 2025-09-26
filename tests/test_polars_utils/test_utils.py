import argparse
import logging
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime
from glob import glob
from pathlib import Path
from unittest.mock import patch

import polars as pl
from polars_utils import utils
from polars_utils.utils import write_to_parquet

SRC_PATH = "polars_utils.validation.actions"


class TestUtils(unittest.TestCase):
    logger = logging.getLogger(__name__)

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
        with self.assertLogs(self.logger) as cm:
            utils.write_to_parquet(df, destination, self.logger, append=False)
        self.assertFalse(destination.exists())
        self.assertTrue(
            "The provided dataframe was empty. No data was written." in cm.output[0]
        )

    def test_write_parquet_writes_simple_dataframe(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        destination = self.temp_dir / "test.parquet"
        with self.assertLogs(self.logger) as cm:
            utils.write_to_parquet(df, destination, self.logger, append=False)
        self.assertTrue(Path(destination).exists())
        self.assertTrue(f"Parquet written to {destination}" in cm.output[0])

    def test_write_parquet_writes_with_append(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 1], "b": [4, 5, 6]})
        destination: str = str(self.temp_dir) + "/"
        write_to_parquet(df, destination, self.logger)
        write_to_parquet(df, destination, self.logger)
        self.assertEqual(len(glob(destination + "/**", recursive=True)), 3)

    def test_write_parquet_writes_with_overwrite(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 1], "b": [4, 5, 6]})
        destination = self.temp_dir / "test.parquet"
        write_to_parquet(df, destination, self.logger, append=False)
        write_to_parquet(df, destination, self.logger, append=False)

        files = glob(os.path.join(self.temp_dir, "*.parquet"))
        self.assertEqual(len(files), 1)

    def test_write_parquet_partitions_complex(self):
        df: pl.DataFrame = pl.read_parquet(
            "tests/test_data/domain=CQC/dataset=delta_locations_api/"
        )

        destination: str = str(self.temp_dir) + "/"
        write_to_parquet(
            df,
            destination,
            self.logger,
            append=False,
            partition_cols=["day", "import_date"],
        )
        files = glob(str(self.temp_dir) + "/**/**/*.parquet")
        self.assertEqual(4, len(files))


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


class TestEmptyS3Folder(unittest.TestCase):
    @patch("boto3.client")
    def test_empty_s3_folder_no_objects(self, mock_s3_client):
        # Given
        paginator = mock_s3_client.return_value.get_paginator.return_value
        paginator.paginate.return_value.search.return_value = [None]
        # When
        with self.assertLogs(level="INFO") as cm:
            utils.empty_s3_folder("my-bucket", "some/prefix/")
        # Then
        self.assertIn(
            "Skipping emptying folder - no objects matching prefix some/prefix/",
            cm.output[0],
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
