import shutil
import tempfile

from polars_utils import utils
import unittest
import polars as pl
from pathlib import Path
import logging
import os

class TestUtils(unittest.TestCase):
    logger = logging.getLogger(__name__)

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_write_parquet_does_nothing_for_empty_dataframe(self):
        df: pl.DataFrame = pl.DataFrame({})
        destination: str = os.path.join(self.temp_dir, 'test.parquet')
        with self.assertLogs(self.logger) as cm:
            utils.write_to_parquet(df, destination, self.logger)
        self.assertFalse(Path(destination).exists())
        self.assertTrue('The provided dataframe was empty. No data was written.' in cm.output[0])

    def test_write_parquet_writes_simple_dataframe(self):
        df: pl.DataFrame = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        destination: str = os.path.join(self.temp_dir, 'test.parquet')
        with self.assertLogs(self.logger) as cm:
            utils.write_to_parquet(df, destination, self.logger)
        self.assertTrue(Path(destination).exists())
        self.assertTrue(f'Parquet written to {destination}' in cm.output[0])

    def test_write_parquet_handles_bad_output_path(self):
        df: pl.DataFrame = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        destination: str = os.path.join('no_dir', 'test.parquet')
        with self.assertLogs(self.logger, level=logging.ERROR) as cm:
            utils.write_to_parquet(df, destination, self.logger)
        self.assertFalse(Path(destination).exists())
        self.assertTrue(f'The destination path is invalid' in cm.output[0])
