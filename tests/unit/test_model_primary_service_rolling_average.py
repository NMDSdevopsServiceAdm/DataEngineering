import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

from utils.estimate_job_count.models.primary_service_rolling_average import (
    model_primary_service_rolling_average,
)


class TestModelPrimaryServiceRollingAverage(unittest.TestCase):
    column_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("primary_service_type", StringType(), False),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 15.0, "Care home with nursing"),
        ("1-000000002", "2023-01-01", 4.0, "non-residential"),
        ("1-000000003", "2023-01-01", 6.0, "non-residential"),
        ("1-000000004", "2023-02-10", 20.0, "non-residential"),
        ("1-000000005", "2023-03-20", 30.0, "non-residential"),
        ("1-000000006", "2023-04-30", 40.0, "non-residential"),
        ("1-000000007", "2023-01-01", None, "non-residential"),
        ("1-000000008", "2023-02-10", None, "non-residential"),
        ("1-000000009", "2023-03-20", None, "non-residential"),
    ]
    # fmt: on

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_model_primary_service_rolling_average"
        ).getOrCreate()
        self.output_df = model_primary_service_rolling_average(
            self.spark.createDataFrame(self.rows, schema=self.column_schema)
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(len(self.rows), self.output_df.count())

    def test_model_primary_service_rolling_averages_are_correct(self):
        pass

    def test_filter_to_locations_with_known_job_count(self):
        pass

    def test_calculate_job_count_sum_and_count_per_service_and_time_period(self):
        pass

    def test_create_rolling_average_column(self):
        pass

    def test_calculate_rolling_sum(self):
        pass

    def test_join_rolling_average_into_df(self):
        pass
