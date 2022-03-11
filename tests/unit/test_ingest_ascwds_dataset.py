from datetime import datetime
from pathlib import Path
from utils import utils
import shutil
import unittest
from pyspark.sql import SparkSession
from jobs import ingest_ascwds_dataset


class IngestASCWDSDatasetTests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("sfc_data_engineering_test_ingest_ascwds_dataset").getOrCreate()

    def test_filter_test_accounts(self):
        columns = [
            "locationid",
            "orgid",
            "location_feature",
        ]
        rows = [
            ("1-000000001", "310", "Definitely a feature"),
            ("1-000000002", "2452", "Not important"),
            ("1-000000003", "308", "Test input"),
            ("1-000000004", "1234", "Something else"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        df = ingest_ascwds_dataset.filter_test_accounts(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["location_feature"], "Something else")
        self.assertEqual(df[0]["locationid"], "1-000000004")


if __name__ == "__main__":
    unittest.main(warnings="ignore")
