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
import time

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
        destination: str = self.temp_dir
        write_to_parquet(df, destination, self.logger)
        write_to_parquet(df, destination, self.logger)
        self.assertEqual(len(glob(destination + "/**", recursive=True)), 3)

    def test_write_parquet_writes_with_overwrite(self):
        df: pl.DataFrame = pl.DataFrame({"a": [1, 2, 1], "b": [4, 5, 6]})
        destination: str = os.path.join(self.temp_dir, "test.parquet")
        write_to_parquet(df, destination, self.logger, append=False)
        write_to_parquet(df, destination, self.logger, append=False)
        self.assertEqual(len(glob(destination + "/**", recursive=True)), 1)

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
