import unittest

from pyspark.sql import SparkSession

import jobs.ingest_ascwds_dataset as job


class IngestASCWDSDatasetTests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "sfc_data_engineering_test_ingest_ascwds_dataset"
        ).getOrCreate()

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

        df = job.filter_test_accounts(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["location_feature"], "Something else")
        self.assertEqual(df[0]["locationid"], "1-000000004")

    def test_filter_test_accounts_without_orgid_doesnt_filter_rows(self):
        columns = [
            "locationid",
            "location_feature",
        ]
        rows = [
            ("1-000000001", "Definitely a feature"),
            ("1-000000002", "Not important"),
            ("1-000000003", "Test input"),
            ("1-000000004", "Something else"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        df = job.filter_test_accounts(df)
        self.assertEqual(df.count(), 4)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
