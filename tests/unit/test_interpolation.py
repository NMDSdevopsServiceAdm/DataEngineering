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

    def test_filter_to_locations_with_a_known_job_count(self):
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)
        filtered_df = job.filter_to_locations_with_a_known_job_count(df)

        self.assertEqual(filtered_df.count(), 5)
        self.assertEqual(filtered_df.columns, ["locationid", "unix_time", "job_count"])

    def test_calculate_first_and_last_submission_date_per_location(self):
        pass

    def test_convert_first_and_last_known_time_into_timeseries_df(self):
        pass

    def test_date_range(self):
        pass

    def test_add_known_job_count_information(self):
        pass

    def test_leftouter_join_on_locationid_and_unix_time(self):
        pass

    def test_add_unix_time_for_known_job_count(self):
        pass

    def test_get_previous_value_in_column(self):
        pass

    def test_get_next_value_in_new_column(self):
        pass

    def test_interpolate_values_for_all_dates(self):
        pass

    def test_input_previous_and_next_values_into_df(self):
        pass

    def test_calculated_interpolated_values_in_new_column(self):
        pass

    def test_interpolation_calculation(self):
        pass
