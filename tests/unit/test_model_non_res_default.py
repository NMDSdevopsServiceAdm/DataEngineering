import unittest
import warnings

from pyspark.sql import SparkSession
from utils.estimate_job_count.models.non_res_default import model_non_res_default


class TestModelNonResDefault(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_estimate_2021_jobs"
        ).getOrCreate()
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_non_res_default(self):
        columns = [
            "locationid",
            "primary_service_type",
            "estimate_job_count",
            "estimate_job_count_source",
        ]
        rows = [
            ("1-000000001", "non-residential", None, None),
            ("1-000000002", "Care home with nursing", None, None),
            ("1-000000003", "non-residential", None, None),
            ("1-000000004", "non-residential", 10, "already_populated"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = model_non_res_default(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_job_count"], 54.09)
        self.assertEqual(df[0]["estimate_job_count_source"], "model_non_res_average")
        self.assertEqual(df[1]["estimate_job_count"], None)
        self.assertEqual(df[1]["estimate_job_count_source"], None)
        self.assertEqual(df[2]["estimate_job_count"], 54.09)
        self.assertEqual(df[3]["estimate_job_count"], 10)
        self.assertEqual(df[3]["estimate_job_count_source"], "already_populated")
