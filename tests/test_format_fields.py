import unittest
from jobs import format_fields
from pyspark.sql import SparkSession
from pathlib import Path
import shutil


class FormatFieldsTests(unittest.TestCase):

    test_csv_path = 'tests/test_data/example_csv.csv'
    tmp_dir = "tmp-out"

    def setUp(self):
        spark = SparkSession.builder \
            .appName("sfc_data_engineering_csv_to_parquet") \
            .getOrCreate()
        self.df = spark.read.csv(self.test_csv_path, header=True)

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            pass  # Ignore dir does not exist

    def test_format_date_fields(self):
        self.assertEqual(self.df.select("date_col").first()[0], "28/11/1993")
        formatted_df = format_fields.format_date_fields(self.df)
        self.assertEqual(str(formatted_df.select(
            "date_col").first()[0]), "1993-01-28 00:11:00")


if __name__ == '__main__':
    unittest.main()
