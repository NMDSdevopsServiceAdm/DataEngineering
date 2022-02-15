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

    def test_collect_ascwds_historical_job_figures(self):
        pass


if __name__ == "__main__":
    unittest.main(warnings="ignore")
