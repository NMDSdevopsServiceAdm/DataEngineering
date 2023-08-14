import unittest
import warnings

from pyspark.sql import SparkSession

import utils.estimate_job_count.models.interpolation as job
from tests.test_file_generator import (
    generate_data_for_interpolation_model,
    generate_data_for_calculating_first_and_last_submission_date_per_location,
    generate_data_for_exploding_dates_into_timeseries_df,
    generate_data_for_merge_known_values_with_exploded_dates_exploded_timeseries_df,
    generate_data_for_merge_known_values_with_exploded_dates_known_ascwds_df,
    generate_data_for_interpolating_values_for_all_dates_df,
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

        self.data_for_merging_exploded_df = (
            generate_data_for_merge_known_values_with_exploded_dates_exploded_timeseries_df()
        )

        self.data_for_merging_known_values_df = (
            generate_data_for_merge_known_values_with_exploded_dates_known_ascwds_df()
        )
        self.data_for_calculating_interpolated_values_df = (
            generate_data_for_interpolating_values_for_all_dates_df()
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

    def test_merge_known_values_with_exploded_dates(self):
        output_df = job.merge_known_values_with_exploded_dates(
            self.data_for_merging_exploded_df, self.data_for_merging_known_values_df
        )

        self.assertEqual(output_df.count(), 5)
        self.assertEqual(
            output_df.columns,
            ["locationid", "unix_time", "job_count", "job_count_unix_time"],
        )

        output_df = output_df.sort("locationid", "unix_time").collect()
        self.assertEqual(output_df[0]["job_count"], None)
        self.assertEqual(output_df[0]["job_count_unix_time"], None)
        self.assertEqual(output_df[1]["job_count"], 1.0)
        self.assertEqual(output_df[1]["job_count_unix_time"], 1672704000)
        self.assertEqual(output_df[2]["job_count"], None)
        self.assertEqual(output_df[2]["job_count_unix_time"], None)
        self.assertEqual(output_df[3]["job_count"], 2.5)
        self.assertEqual(output_df[3]["job_count_unix_time"], 1672876800)
        self.assertEqual(output_df[4]["job_count"], 15.0)
        self.assertEqual(output_df[4]["job_count_unix_time"], 1672790400)

    def test_interpolate_values_for_all_dates(self):
        output_df = job.interpolate_values_for_all_dates(
            self.data_for_calculating_interpolated_values_df
        )

        self.assertEqual(output_df.count(), 8)
        self.assertEqual(
            output_df.columns,
            ["locationid", "unix_time", "interpolation_model"],
        )

        output_df = output_df.sort("locationid").collect()
        self.assertEqual(output_df[0]["interpolation_model"], 30.0)
        self.assertEqual(output_df[1]["interpolation_model"], 4.0)
        self.assertEqual(output_df[2]["interpolation_model"], 4.5)
        self.assertEqual(output_df[3]["interpolation_model"], 5.0)
        self.assertEqual(output_df[4]["interpolation_model"], 5.0)
        self.assertAlmostEqual(output_df[5]["interpolation_model"], 6.1666, places=3)
        self.assertAlmostEqual(output_df[6]["interpolation_model"], 7.3333, places=3)
        self.assertEqual(output_df[7]["interpolation_model"], 8.5)
