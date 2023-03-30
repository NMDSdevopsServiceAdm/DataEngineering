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

import utils.estimate_job_count.models.extrapolation as job


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
        self.spark = SparkSession.builder.appName("test_extrapolation").getOrCreate()
        self.non_res_model_df = job.model_extrapolation(
            self.spark.createDataFrame(self.rows, schema=self.column_schema)
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_non_res_row_count_unchanged(self):

        self.assertEqual(self.non_res_model_df.count(), len(self.rows))

    def test_convert_date_to_unix_timestamp(self):
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        df = job.convert_date_to_unix_timestamp(
            df, "snapshot_date", "yyyy-MM-dd", "snapshot_date_unix_conv"
        )

        df = df.orderBy("locationid").collect()
        self.assertEqual(df[0]["snapshot_date_unix_conv"], 1672531200)

    def test_rolling_total(self):
        pass

    def test_convert_days_to_unix_time(self):
        self.assertEqual(job.convert_days_to_unix_time(1), 86400)
        self.assertEqual(job.convert_days_to_unix_time(90), 7776000)

    def test_create_rolling_average_column(self):
        pass
