import unittest
import warnings

from pyspark.sql import SparkSession

import utils.estimate_job_count.models.extrapolation as job
from tests.test_file_generator import (
    generate_data_for_extrapolation_model,
    generate_data_for_extrapolation_location_filtering_df,
    generate_data_for_job_count_and_rolling_average_first_and_last_submissions_df,
)


class TestModelExtrapolation(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_extrapolation").getOrCreate()
        self.extrapolation_df = generate_data_for_extrapolation_model()
        self.data_to_filter_df = generate_data_for_extrapolation_location_filtering_df()
        self.data_for_first_and_last_submissions_df = (
            generate_data_for_job_count_and_rolling_average_first_and_last_submissions_df()
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_extrapolation_row_count_unchanged(self):
        output_df = job.model_extrapolation(self.extrapolation_df)
        self.assertEqual(output_df.count(), self.extrapolation_df.count())

    def test_filter_to_locations_who_have_a_job_count_at_some_point(self):
        output_df = job.filter_to_locations_who_have_a_job_count_at_some_point(
            self.data_to_filter_df
        )

        self.assertEqual(output_df.count(), 3)
        self.assertEqual(
            output_df.columns,
            [
                "locationid",
                "max_job_count",
                "snapshot_date",
                "job_count",
                "primary_service_type",
            ],
        )

        output_df = output_df.sort("locationid", "snapshot_date").collect()
        self.assertEqual(output_df[0]["locationid"], "1-000000001")
        self.assertEqual(output_df[0]["max_job_count"], 15.0)
        self.assertEqual(output_df[1]["locationid"], "1-000000003")
        self.assertEqual(output_df[1]["max_job_count"], 20.0)
        self.assertEqual(output_df[2]["locationid"], "1-000000003")
        self.assertEqual(output_df[2]["max_job_count"], 20.0)

    def test_add_first_and_last_submission_date_cols(self):
        output_df = job.add_first_and_last_submission_date_cols(
            self.data_for_first_and_last_submissions_df
        )

        self.assertEqual(output_df.count(), 6)

        output_df = output_df.sort("locationid", "unix_time").collect()
        self.assertEqual(output_df[0][job.FIRST_SUBMISSION_TIME], 1675209600)
        self.assertEqual(output_df[0][job.LAST_SUBMISSION_TIME], 1675209600)
        self.assertEqual(output_df[2][job.FIRST_SUBMISSION_TIME], 1675209600)
        self.assertEqual(output_df[2][job.LAST_SUBMISSION_TIME], 1675209600)
        self.assertEqual(output_df[3][job.FIRST_SUBMISSION_TIME], 1672531200)
        self.assertEqual(output_df[3][job.LAST_SUBMISSION_TIME], 1675209600)
        self.assertEqual(output_df[5][job.FIRST_SUBMISSION_TIME], 1672531200)
        self.assertEqual(output_df[5][job.LAST_SUBMISSION_TIME], 1675209600)
