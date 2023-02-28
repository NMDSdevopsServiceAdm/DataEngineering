import unittest
import warnings

from pyspark.sql import SparkSession
from utils.estimate_job_count.models.non_res_rolling_three_month_average import (
    model_non_res_rolling_three_month_average,
)


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
            "snapshot_date",
            "job_count",
            "primary_service_type",
            "estimate_job_count",
            "estimate_job_count_source",
        ]
        # fmt: off
        rows = [
            ("1-000000001", "2023-01-01", 15, "Care home with nursing", None, None),
            ("1-000000002", "2023-01-01", 5, "non-residential", None, None),
            ("1-000000003", "2023-01-01", 5, "non-residential", None, None),
            ("1-000000004", "2023-02-10", 20, "non-residential", None, None),
            ("1-000000005", "2023-03-20", 30, "non-residential", 30, "already_populated",),
            ("1-000000006", "2023-04-30", 40, "non-residential", None, None,),
        ]
        # fmt: on
        df = self.spark.createDataFrame(rows, columns)

        df = model_non_res_rolling_three_month_average(df)
        self.assertEqual(df.count(), 6)

        df = df.orderBy("locationid").collect()
        self.assertEqual(df[0]["estimate_job_count"], None)
        self.assertEqual(df[0]["model_non_res_rolling_three_month_average"], None)
        self.assertEqual(df[0]["estimate_job_count_source"], None)

        self.assertEqual(df[1]["estimate_job_count"], 5.0)
        self.assertEqual(df[1]["model_non_res_rolling_three_month_average"], 5.0)
        self.assertEqual(
            df[1]["estimate_job_count_source"],
            "model_non_res_rolling_three_month_average",
        )

        self.assertEqual(df[2]["estimate_job_count"], 5.0)
        self.assertEqual(df[2]["model_non_res_rolling_three_month_average"], 5.0)
        self.assertEqual(
            df[2]["estimate_job_count_source"],
            "model_non_res_rolling_three_month_average",
        )

        self.assertEqual(df[3]["estimate_job_count"], 10.0)
        self.assertEqual(df[3]["model_non_res_rolling_three_month_average"], 10.0)
        self.assertEqual(
            df[3]["estimate_job_count_source"],
            "model_non_res_rolling_three_month_average",
        )

        self.assertEqual(df[4]["estimate_job_count"], 30.0)
        self.assertEqual(df[4]["model_non_res_rolling_three_month_average"], 15.0)
        self.assertEqual(df[4]["estimate_job_count_source"], "already_populated")

        self.assertEqual(df[5]["estimate_job_count"], 30.0)
        self.assertEqual(df[5]["model_non_res_rolling_three_month_average"], 30.0)
        self.assertEqual(
            df[5]["estimate_job_count_source"],
            "model_non_res_rolling_three_month_average",
        )
