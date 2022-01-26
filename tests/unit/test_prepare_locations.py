import unittest
from jobs import format_fields
from jobs import prepare_locations
from pyspark.sql import SparkSession
from pathlib import Path
import shutil


class PrepareLocationsTests(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("test_prepare_locations") \
            .getOrCreate()

    def test_filter_nulls(self):

        columns = ["locationid", "wkrrecs", "totalstaff"]
        rows = [
            ("1-000000001", None, 20),
            ("1-000000002", 500, 500),
            ("1-000000003", 100, None),
            ("1-000000004", None, None),
            ("1-000000005", 25, 75),
            (None, 1, 0),
        ]
        df = self.spark.createDataFrame(rows, columns)

        filtered_df = prepare_locations.filter_nulls(df)
        self.assertEqual(filtered_df.count(), 4)
        self.assertEqual(filtered_df.select("locationid").rdd.flatMap(lambda x: x).collect(), [
                         "1-000000001", "1-000000002", "1-000000003", "1-000000005"])

    def test_clean(self):

        columns = ["locationid", "wkrrecs", "totalstaff"]
        rows = [
            ("1-000000001", None, "0"),
            ("1-000000002", "500", "500"),
            ("1-000000003", "100", "-1"),
            ("1-000000004", None, "0"),
            ("1-000000005", "25", "75"),
            (None, "1", "0"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        cleaned_df = prepare_locations.clean(df)
        self.assertEqual(cleaned_df.count(), 6)


if __name__ == '__main__':
    unittest.main()
