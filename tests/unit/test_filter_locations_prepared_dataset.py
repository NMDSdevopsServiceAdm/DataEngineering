import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from utils.estimate_job_count.filter_locations_prepared_dataset import (
    filter_to_only_cqc_independent_sector_data,
)


class TestFilterLocationsPrepared(unittest.TestCase):
    filter_locations_prepared_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("cqc_sector", StringType(), False),
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_filter_locations_prepared"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_filter_to_only_cqc_independent_sector_data_returns_only_independent_sector_rows(
        self,
    ):
        rows = [
            ("1-000000001", "Independent"),
            ("1-000000002", "Local authority"),
            ("1-000000003", ""),
        ]
        df = self.spark.createDataFrame(
            rows, schema=self.filter_locations_prepared_schema
        )

        filtered_df = filter_to_only_cqc_independent_sector_data(df)

        self.assertEqual(filtered_df.count(), 1)
