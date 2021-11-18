import unittest
from jobs import csv_to_parquet
from pyspark.sql import SparkSession
from pathlib import Path
import shutil


class CSVToParquetTests(unittest.TestCase):

    test_csv_path = 'tests/test_data/example_csv.csv'
    test_csv_custom_delim_path = 'tests/test_data/example_csv_custom_delimiter.csv'
    tmp_dir = "tmp-out"

    def setUp(self):
        pass

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            pass  # Ignore dir does not exist

    def test_read(self):
        df = csv_to_parquet.read_csv(self.test_csv_path)
        self.assertEqual(df.columns, ["col_a", "col_b", "col_c"])
        self.assertEqual(df.count(), 3)

    def test_read_with_custom_delimiter(self):
        df = csv_to_parquet.read_csv(self.test_csv_custom_delim_path, "|")

        self.assertEqual(df.columns, ["col_a", "col_b", "col_c"])
        self.assertEqual(df.count(), 3)

    def test_write(self):
        df = csv_to_parquet.read_csv(self.test_csv_path)
        csv_to_parquet.write_parquet(df, self.tmp_dir)

        self.assertTrue(Path("tmp-out").is_dir())
        self.assertTrue(Path("tmp-out/_SUCCESS").exists())


if __name__ == '__main__':
    unittest.main()
