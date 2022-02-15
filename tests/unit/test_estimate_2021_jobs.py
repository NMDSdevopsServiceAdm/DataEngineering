import datetime
import shutil
import unittest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from jobs import estimate_2021_jobs


class Estimate2021JobsTests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_estimate_2021_jobs").getOrCreate()

    def test_determine_ascwds_primary_service_type(self):
        columns = ["locationid", "services"]
        rows = [
            ("1-000000001", ["Care home service with nursing", "Care home service without nursing", "Fake service"]),
            ("1-000000002", ["Care home service without nursing", "Fake service"]),
            ("1-000000003", ["Fake service"]),
            ("1-000000003", []),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = estimate_2021_jobs.determine_ascwds_primary_service_type(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["primary_service_type"], "Care home with nursing")
        self.assertEqual(df[1]["primary_service_type"], "Care home without nursing")
        self.assertEqual(df[2]["primary_service_type"], "non-residential")
        self.assertEqual(df[3]["primary_service_type"], "non-residential")

    def test_model_populate_known_2021_jobs(self):
        columns = ["locationid", "jobcount_2021", "estimate_jobcount_2021"]
        rows = [
            ("1-000000001", 1, None),
            ("1-000000002", None, None),
            ("1-000000003", 5, 4),
            ("1-000000003", 10, None),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = estimate_2021_jobs.model_populate_known_2021_jobs(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 1)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], 4)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)

    def test_model_non_res_historical(self):
        columns = [
            "locationid",
            "primary_service_type",
            "last_known_job_count",
            "estimate_jobcount_2021",
        ]
        rows = [
            ("1-000000001", "non-residential", 10, None),
            ("1-000000002", "Care hom", 10, None),
            ("1-000000003", "non-residential", 10, None),
            ("1-000000004", "non-residential", 10, None),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = estimate_2021_jobs.model_populate_known_2021_jobs(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 1)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], 4)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
