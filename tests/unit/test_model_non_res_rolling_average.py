import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from utils.estimate_job_count.models.non_res_rolling_average import (
    model_non_res_rolling_average,
)


class TestModelNonResDefault(unittest.TestCase):
    column_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("job_count", IntegerType(), True),
            StructField("primary_service_type", StringType(), False),
            StructField("estimate_job_count", DoubleType(), True),
            StructField("estimate_job_count_source", StringType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 15, "Care home with nursing", None, None),
        ("1-000000002", "2023-01-01", 4, "non-residential", None, None),
        ("1-000000003", "2023-01-01", 6, "non-residential", None, None),
        ("1-000000004", "2023-02-10", 20, "non-residential", None, None),
        ("1-000000005", "2023-03-20", 30, "non-residential", 30.0, "already_populated",),
        ("1-000000006", "2023-04-30", 40, "non-residential", None, None,),
        ("1-000000007", "2023-01-01", None, "non-residential", None, None,),
        ("1-000000008", "2023-02-10", None, "non-residential", None, None,),
        ("1-000000009", "2023-03-20", None, "non-residential", 30.0, "already_populated",),
    ]
    # fmt: on

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_estimate_2021_jobs").getOrCreate()
        self.non_res_model_df = model_non_res_rolling_average(
            self.spark.createDataFrame(self.rows, schema=self.column_schema)
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_non_res_row_count_unchanged(self):

        self.assertEqual(self.non_res_model_df.count(), len(self.rows))

    def test_model_non_res_rolling_average_returns_none_if_not_non_res(self):

        df = self.non_res_model_df.orderBy("locationid").collect()
        self.assertEqual(df[0]["estimate_job_count"], None)
        # self.assertEqual(df[0]["model_non_res_rolling_average"], None)
        self.assertEqual(df[0]["estimate_job_count_source"], None)

    def test_model_non_res_rolling_average_returns_average_when_job_count_populated_and_estimate_is_none(
        self,
    ):

        df = self.non_res_model_df.orderBy("locationid").collect()
        self.assertEqual(df[1]["estimate_job_count"], 5.0)
        self.assertEqual(df[1]["model_non_res_rolling_average"], 5.0)
        self.assertEqual(
            df[1]["estimate_job_count_source"],
            "model_non_res_rolling_average",
        )

        self.assertEqual(df[2]["estimate_job_count"], 5.0)
        self.assertEqual(df[2]["model_non_res_rolling_average"], 5.0)
        self.assertEqual(
            df[2]["estimate_job_count_source"],
            "model_non_res_rolling_average",
        )

        self.assertEqual(df[3]["estimate_job_count"], 10.0)
        self.assertEqual(df[3]["model_non_res_rolling_average"], 10.0)
        self.assertEqual(
            df[3]["estimate_job_count_source"],
            "model_non_res_rolling_average",
        )

        self.assertEqual(df[5]["estimate_job_count"], 30.0)
        self.assertEqual(df[5]["model_non_res_rolling_average"], 30.0)
        self.assertEqual(
            df[5]["estimate_job_count_source"],
            "model_non_res_rolling_average",
        )

    def test_model_non_res_rolling_average_returns_average_when_job_count_populated_but_doesnt_replace_estimate(
        self,
    ):

        df = self.non_res_model_df.orderBy("locationid").collect()
        self.assertEqual(df[4]["estimate_job_count"], 30.0)
        self.assertEqual(df[4]["model_non_res_rolling_average"], 15.0)
        self.assertEqual(df[4]["estimate_job_count_source"], "already_populated")

    def test_model_non_res_rolling_average_returns_average_when_job_count_is_none(self):

        df = self.non_res_model_df.orderBy("locationid").collect()
        self.assertEqual(df[6]["estimate_job_count"], 5.0)
        self.assertEqual(df[6]["model_non_res_rolling_average"], 5.0)
        self.assertEqual(
            df[6]["estimate_job_count_source"],
            "model_non_res_rolling_average",
        )

        self.assertEqual(df[7]["estimate_job_count"], 10.0)
        self.assertEqual(df[7]["model_non_res_rolling_average"], 10.0)
        self.assertEqual(
            df[7]["estimate_job_count_source"],
            "model_non_res_rolling_average",
        )

    def test_model_non_res_rolling_average_returns_average_when_job_count_is_none_but_doesnt_replace_estimate(
        self,
    ):

        df = self.non_res_model_df.orderBy("locationid").collect()
        self.assertEqual(df[8]["estimate_job_count"], 30.0)
        self.assertEqual(df[8]["model_non_res_rolling_average"], 15.0)
        self.assertEqual(df[8]["estimate_job_count_source"], "already_populated")
