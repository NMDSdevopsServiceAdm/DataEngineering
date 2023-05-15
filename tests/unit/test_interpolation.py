import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)

import utils.estimate_job_count.models.interpolation as job


class TestModelInterpolation(unittest.TestCase):
    column_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("estimate_job_count", DoubleType(), True),
            StructField("estimate_job_count_source", StringType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 1672531200, None, None, None),
        ("1-000000001", "2023-01-02", 1672617600, 30.0, 30.0, "ascwds_job_count"),
        ("1-000000001", "2023-01-03", 1672704000, None, None, None),
        ("1-000000002", "2023-01-01", 1672531200, None, None, None),
        ("1-000000002", "2023-01-03", 1672704000, 4.0, 4.0, "ascwds_job_count"),
        ("1-000000002", "2023-01-05", 1672876800, None, None, None),
        ("1-000000002", "2023-01-07", 1673049600, 5.0, 5.0, "ascwds_job_count"),
        ("1-000000002", "2023-01-09", 1673222400, 5.0, 5.0, "ascwds_job_count"),
        ("1-000000002", "2023-01-11", 1673395200, None, None, None),
        ("1-000000002", "2023-01-13", 1673568000, None, None, None),
        ("1-000000002", "2023-01-15", 1673740800, 20.0, 20.0, "ascwds_job_count"),
        ("1-000000002", "2023-01-17", 1673913600, None, 21.0, "other_source"),
        ("1-000000002", "2023-01-19", 1674086400, None, None, None),
    ]
    # fmt: on

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_interpolation").getOrCreate()
        self.interpolation_df = job.model_interpolation(
            self.spark.createDataFrame(self.rows, schema=self.column_schema)
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_interpolation_row_count_unchanged(self):

        self.assertEqual(self.interpolation_df.count(), len(self.rows))
