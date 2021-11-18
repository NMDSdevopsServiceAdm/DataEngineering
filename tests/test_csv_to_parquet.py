"""
test_csv_to_parquet.py
"""
import unittest
from jobs import csv_to_parquet
from pyspark.sql import SparkSession
from pathlib import Path
import shutil


class CSVToParquetTests(unittest.TestCase):

    test_data_path = 'tests/test_data/example_csv.csv'
    tmp_dir = "tmp-out"

    def setUp(self):
        pass

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))

    def test_extract_csv(self):
        csv_to_parquet.main(self.test_data_path, self.tmp_dir)
        self.assertTrue(Path("tmp-out").is_dir())
        self.assertTrue(Path("tmp-out/_SUCCESS").exists())


if __name__ == '__main__':
    unittest.main()
