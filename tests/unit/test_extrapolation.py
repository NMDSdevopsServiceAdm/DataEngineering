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

import utils.estimate_job_count.models.extrapolation as job


class TestModelNonResDefault(unittest.TestCase):
    column_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("primary_service_type", StringType(), False),
            StructField("estimate_job_count", DoubleType(), True),
            StructField("estimate_job_count_source", StringType(), True),
            StructField("rolling_average_model", DoubleType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 1672531200, 15.0, "Care home with nursing", None, None, 15.0),
        ("1-000000001", "2023-02-01", 1675209600, None, "Care home with nursing", None, None, 15.1),
        ("1-000000001", "2023-03-01", 1677628800, 30.0, "Care home with nursing", 30.0, "already_populated", 15.2),
        ("1-000000002", "2023-01-01", 1672531200, 4.0, "non-residential", None, None, 50.3),
        ("1-000000002", "2023-02-01", 1675209600, None, "non-residential", None, None, 50.5),
        ("1-000000002", "2023-03-01", 1677628800, None, "non-residential", 5.0, "already_populated", 50.7),
        ("1-000000002", "2023-04-01", 1680303600, None, "non-residential", None, None, 50.1),
        ("1-000000003", "2023-01-01", 1672531200, None, "non-residential", None, None, 50.3),
        ("1-000000003", "2023-02-01", 1675209600, 20.0, "non-residential", None, None, 50.5),
        ("1-000000003", "2023-03-01", 1677628800, None, "non-residential", 30.0, "already_populated", 50.7),
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
