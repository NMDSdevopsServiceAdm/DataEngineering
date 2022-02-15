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

from jobs import estimate_2021_jobs as job


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

        df = job.determine_ascwds_primary_service_type(df)
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

        df = job.model_populate_known_2021_jobs(df)
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
            ("1-000000002", "Care home with nursing", 10, None),
            ("1-000000003", "non-residential", 20, None),
            ("1-000000004", "non-residential", 10, 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_non_res_historical(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 10.3)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], 20.6)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)

    def test_model_non_res_historical_pir(self):
        columns = [
            "locationid",
            "primary_service_type",
            "last_known_job_count",
            "pir_service_users",
            "estimate_jobcount_2021",
        ]
        rows = [
            ("1-000000001", "non-residential", 10, 5, None),
            ("1-000000002", "Care home without nursing", 10, 3, None),
            ("1-000000003", "non-residential", 10, 10, None),
            ("1-000000004", "non-residential", 10, None, 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_non_res_historical_pir(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 27.391)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], 29.735999999999997)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)

    def test_model_non_res_default(self):
        columns = [
            "locationid",
            "primary_service_type",
            "estimate_jobcount_2021",
        ]
        rows = [
            ("1-000000001", "non-residential", None),
            ("1-000000002", "Care home with nursing", None),
            ("1-000000003", "non-residential", None),
            ("1-000000004", "non-residential", 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_non_res_default(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 54.09)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], 54.09)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)

    def test_model_care_home_with_nursing_historical(self):
        columns = [
            "locationid",
            "primary_service_type",
            "last_known_job_count",
            "estimate_jobcount_2021",
        ]
        rows = [
            ("1-000000001", "Care home with nursing", 10, None),
            ("1-000000002", "Care home without nursing", 10, None),
            ("1-000000003", "Care home with nursing", 20, None),
            ("1-000000004", "non-residential", 10, 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_care_home_with_nursing_historical(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 10.04)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], 20.08)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)

    def test_model_care_home_with_nursing_pir_and_cqc_beds(self):
        columns = [
            "locationid",
            "primary_service_type",
            "pir_service_users",
            "cqc_number_of_beds",
            "estimate_jobcount_2021",
        ]
        rows = [
            ("1-000000001", "Care home with nursing", 10, 10, None),
            ("1-000000002", "Care home without nursing", 10, 5, None),
            ("1-000000003", "Care home with nursing", 20, None, None),
            ("1-000000004", "non-residential", 10, None, 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_care_home_with_nursing_pir_and_cqc_beds(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 13.544000000000002)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], None)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)

    def test_model_care_home_with_nursing_cqc_beds(self):
        columns = [
            "locationid",
            "primary_service_type",
            "cqc_number_of_beds",
            "estimate_jobcount_2021",
        ]
        rows = [
            ("1-000000001", "Care home with nursing", 10, None),
            ("1-000000002", "Care home without nursing", 5, None),
            ("1-000000003", "Care home with nursing", 5, None),
            ("1-000000004", "non-residential", 10, 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_care_home_with_nursing_cqc_beds(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 14.420000000000002)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], 8.405000000000001)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)

    def test_model_care_home_without_nursing_historical(self):
        columns = [
            "locationid",
            "primary_service_type",
            "last_known_job_count",
            "estimate_jobcount_2021",
        ]
        rows = [
            ("1-000000001", "Care home without nursing", 10, None),
            ("1-000000002", "Care home with nursing", 10, None),
            ("1-000000003", "Care home without nursing", 20, None),
            ("1-000000004", "non-residential", 10, 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_care_home_without_nursing_historical(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 10.1)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], 20.2)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)

    def test_model_care_home_without_nursing_cqc_beds_and_pir(self):
        columns = [
            "locationid",
            "primary_service_type",
            "pir_service_users",
            "cqc_number_of_beds",
            "estimate_jobcount_2021",
        ]
        rows = [
            ("1-000000001", "Care home without nursing", 10, 5, None),
            ("1-000000002", "Care home with nursing", 10, 3, None),
            ("1-000000003", "Care home without nursing", 20, None, None),
            ("1-000000004", "non-residential", 10, None, 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_care_home_without_nursing_cqc_beds_and_pir(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 16.467)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], None)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)

    def test_model_care_home_without_nursing_cqc_beds(self):
        columns = [
            "locationid",
            "primary_service_type",
            "cqc_number_of_beds",
            "estimate_jobcount_2021",
        ]
        rows = [
            ("1-000000001", "Care home without nursing", 10, None),
            ("1-000000002", "Care home with nursing", 10, None),
            ("1-000000003", "Care home without nursing", 20, None),
            ("1-000000004", "non-residential", 10, 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_care_home_without_nursing_cqc_beds(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_jobcount_2021"], 19.417)
        self.assertEqual(df[1]["estimate_jobcount_2021"], None)
        self.assertEqual(df[2]["estimate_jobcount_2021"], 27.543)
        self.assertEqual(df[3]["estimate_jobcount_2021"], 10)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
