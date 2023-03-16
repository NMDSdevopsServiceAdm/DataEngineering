import unittest
import warnings

from pyspark.sql import SparkSession

from utils.prepare_locations_utils.filter_job_count.care_home_jobs_per_bed_ratio_outliers import (
    care_home_jobs_per_bed_ratio_outliers,
    select_relevant_data,
)
from tests.test_file_generator import generate_care_home_jobs_per_bed_filter_df


class EstimateJobCountTests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_filter_job_count").getOrCreate()
        self.prepared_locations_input_data = generate_care_home_jobs_per_bed_filter_df()
        self.filtered_output_df = care_home_jobs_per_bed_ratio_outliers(
            self.prepared_locations_input_data, "job_count_unfiltered", "job_count"
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_overall_output_df_has_same_number_of_rows_as_input_df(self):

        self.assertEqual(
            self.prepared_locations_input_data.count(), self.filtered_output_df.count()
        )

    def test_relevant_date_selected(self):

        df = select_relevant_data(
            self.prepared_locations_input_data, "job_count_unfiltered"
        )
        self.assertEqual(df.count(), 42)

    def test_select_data_not_in_subset_df(self):

        pass

    def test_calculate_jobs_per_bed_ratio(self):

        pass

    def test_create_banded_bed_count_column(self):

        pass

    def test_calculate_average_jobs_per_banded_bed_count(self):

        pass

    def test_calculate_standardised_residuals(self):

        pass

    def test_calculate_expected_jobs_based_on_number_of_beds(self):

        pass

    def test_calculate_job_count_residuals(self):

        pass

    def test_calculate_job_count_standardised_residual(self):

        pass

    def test_calculate_standardised_residual_cutoffs(self):

        pass

    def test_calculate_percentile(self):

        pass

    def test_create_filtered_job_count_df(self):

        pass

    def test_join_filtered_col_into_care_home_df(self):

        pass

    def test_add_job_counts_without_filtering_to_data_outside_of_this_filter(self):

        pass

    def test_combine_dataframes(self):

        pass

    def test_round_figures_in_column(self):

        pass
