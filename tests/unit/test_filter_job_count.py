import unittest
import warnings

from pyspark.sql import SparkSession

from utils.prepare_locations_utils.filter_job_count.filter_job_count import (
    filter_job_count,
)
from tests.test_file_generator import generate_care_home_jobs_per_bed_filter_df


class EstimateJobCountTests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_filter_job_count").getOrCreate()
        self.prepared_locations_input_data = generate_care_home_jobs_per_bed_filter_df()
        self.filtered_output_df = filter_job_count(
            self.prepared_locations_input_data, "job_count_unfiltered", "job_count"
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_overall_output_df_has_same_number_of_rows_as_input_df(self):

        self.assertEqual(
            self.prepared_locations_input_data.count(), self.filtered_output_df.count()
        )
