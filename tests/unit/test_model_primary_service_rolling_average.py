import unittest
import warnings

from pyspark.sql import SparkSession

from utils.estimate_job_count.models.primary_service_rolling_average import (
    model_primary_service_rolling_average,
)
from tests.test_file_generator import input_data_for_primary_service_rolling_average


class TestModelPrimaryServiceRollingAverage(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_model_primary_service_rolling_average"
        ).getOrCreate()
        self.input_df = input_data_for_primary_service_rolling_average()
        self.output_df = model_primary_service_rolling_average(
            self.spark.createDataFrame(self.rows, schema=self.column_schema)
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(self.input_df.count(), self.output_df.count())

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
