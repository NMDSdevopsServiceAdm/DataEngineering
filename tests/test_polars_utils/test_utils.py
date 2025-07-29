from polars_utils import utils
import unittest
import polars as pl
from pathlib import Path
import logging

class TestUtils(unittest.TestCase):
    logger = logging.getLogger(__name__)

    def test_write_parquet_does_nothing_for_empty_dataframe(self):
        df: pl.DataFrame = pl.DataFrame({})
        destination: str = '/tmp/test.parquet'
        with self.assertLogs(self.logger) as cm:
            utils.write_to_parquet(df, destination, self.logger)
        self.assertFalse(Path(destination).exists())
        self.assertTrue('The provided dataframe was empty. No data was written.' in cm.output[0])

