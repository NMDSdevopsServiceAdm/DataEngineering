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
            utils.write_to_parquet(df, destination, self.logger)
        self.assertFalse(Path(destination).exists())
        self.assertTrue(
            "The provided dataframe was empty. No data was written." in cm.output[0]
        )

    def test_write_parquet_writes_simple_dataframe(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        destination: str = os.path.join(self.temp_dir, "test.parquet")
        with self.assertLogs(self.logger) as cm:
            utils.write_to_parquet(df, destination, self.logger)
        self.assertTrue(Path(destination).exists())
        self.assertTrue(f"Parquet written to {destination}" in cm.output[0])

    def test_write_parquet_writes_with_append(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 1], "b": [4, 5, 6]})
        destination: str = self.temp_dir
        write_to_parquet(df, destination, self.logger, append=True)
        write_to_parquet(df, destination, self.logger, append=True)
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
