import unittest
import warnings

from pyspark.sql import SparkSession

import utils.estimate_job_count.models.interpolation as job
from tests.test_file_generator import (
    generate_data_for_interpolation_model,
    generate_data_for_calculating_first_and_last_submission_date_per_location,
    generate_data_for_exploding_dates_into_timeseries_df,
)


class TestModelInterpolation(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_interpolation").getOrCreate()
        self.interpolation_df = generate_data_for_interpolation_model()
        self.data_for_calculating_submission_dates = (
            generate_data_for_calculating_first_and_last_submission_date_per_location()
        )
        self.data_for_creating_timeseries_df = (
            generate_data_for_exploding_dates_into_timeseries_df()
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_interpolation_row_count_unchanged(self):
        df = job.model_interpolation(self.interpolation_df)
        self.assertEqual(df.count(), self.interpolation_df.count())

    def test_filter_to_locations_with_a_known_job_count(self):
        filtered_df = job.filter_to_locations_with_a_known_job_count(
            self.interpolation_df
        )

        self.assertEqual(filtered_df.count(), 5)
        self.assertEqual(filtered_df.columns, ["locationid", "unix_time", "job_count"])

    def test_calculate_first_and_last_submission_date_per_location(self):
        output_df = job.calculate_first_and_last_submission_date_per_location(
            self.data_for_calculating_submission_dates
        )

        self.assertEqual(output_df.count(), 2)
        self.assertEqual(
            output_df.columns,
            ["locationid", "first_submission_time", "last_submission_time"],
        )

        output_df = output_df.sort("locationid").collect()
        self.assertEqual(output_df[0]["first_submission_time"], 1672617600)
        self.assertEqual(output_df[0]["last_submission_time"], 1672617600)
        self.assertEqual(output_df[1]["first_submission_time"], 1672704000)
        self.assertEqual(output_df[1]["last_submission_time"], 1673222400)

    def test_convert_first_and_last_known_years_into_exploded_df(self):
        df = job.convert_first_and_last_known_years_into_exploded_df(
            self.data_for_creating_timeseries_df
        )

        self.assertEqual(df.count(), 6)
        self.assertEqual(
            df.columns,
            ["locationid", "unix_time"],
        )
