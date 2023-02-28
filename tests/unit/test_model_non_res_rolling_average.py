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
    convert_date_to_unix_timestamp,
    convert_days_to_unix_time,
    create_non_res_rolling_average_column,
    ROLLING_AVERAGE_TIME_PERIOD_IN_DAYS,
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
        self.spark = SparkSession.builder.appName(
            "test_estimate_2021_jobs"
        ).getOrCreate()
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_non_res_row_count_unchanged(self):
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        df = model_non_res_rolling_average(df)

        self.assertEqual(df.count(), self.rows.count())

    def test_model_non_res_rolling_average_returns_none_if_not_non_res(self):
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        df = model_non_res_rolling_average(df)

        df = df.orderBy("locationid").collect()
        self.assertEqual(df[0]["estimate_job_count"], None)
        self.assertEqual(df[0]["model_non_res_rolling_average"], None)
        self.assertEqual(df[0]["estimate_job_count_source"], None)

    def test_model_non_res_rolling_average_returns_average_when_job_count_populated_and_estimate_is_none(
        self,
    ):
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        df = model_non_res_rolling_average(df)

        df = df.orderBy("locationid").collect()
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
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        df = model_non_res_rolling_average(df)

        df = df.orderBy("locationid").collect()
        self.assertEqual(df[4]["estimate_job_count"], 30.0)
        self.assertEqual(df[4]["model_non_res_rolling_average"], 15.0)
        self.assertEqual(df[4]["estimate_job_count_source"], "already_populated")

    def test_model_non_res_rolling_average_returns_average_when_job_count_is_none(self):
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        df = model_non_res_rolling_average(df)

        df = df.orderBy("locationid").collect()
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
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        df = model_non_res_rolling_average(df)

        df = df.orderBy("locationid").collect()
        self.assertEqual(df[8]["estimate_job_count"], 30.0)
        self.assertEqual(df[8]["model_non_res_rolling_average"], 15.0)
        self.assertEqual(df[8]["estimate_job_count_source"], "already_populated")

    def test_convert_date_to_unix_timestamp(self):
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        df = convert_date_to_unix_timestamp(
            df, "snapshot_date", "yyyy-MM-dd", "snapshot_date_unix_conv"
        )

        df = df.orderBy("locationid").collect()
        self.assertEqual(df[0]["snapshot_date_unix_conv"], 1672531200)

    def test_convert_days_to_unix_time(self):
        self.assertEqual(convert_days_to_unix_time(1), 86400)
        self.assertEqual(convert_days_to_unix_time(90), 7776000)

    def test_create_non_res_rolling_average_column_removes_care_home_column(self):
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        df = create_non_res_rolling_average_column(
            df, ROLLING_AVERAGE_TIME_PERIOD_IN_DAYS
        )

        self.assertEqual(df.count(), 8)

    def test_create_non_res_rolling_average_column_calculates_prediction_column(self):
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        df = create_non_res_rolling_average_column(
            df, ROLLING_AVERAGE_TIME_PERIOD_IN_DAYS
        )

        df = df.orderBy("locationid").collect()
        self.assertEqual(df[0]["prediction"], 5.0)
        self.assertEqual(df[2]["prediction"], 10.0)
        self.assertEqual(df[4]["prediction"], 30.0)
        self.assertEqual(df[6]["prediction"], 10.0)
