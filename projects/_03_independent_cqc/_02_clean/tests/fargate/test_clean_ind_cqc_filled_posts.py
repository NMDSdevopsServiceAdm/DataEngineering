import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import polars as pl

import projects._03_independent_cqc._02_clean.fargate.clean_ind_cqc_filled_posts as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CleanIndCQCData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CleanIndCQCSchema as Schemas,
)

PATCH_PATH = "projects._03_independent_cqc._02_clean.fargate.clean_ind_cqc_filled_posts"


class CleanIndFilledPostsTests(unittest.TestCase):
    MERGE_IND_CQC_SOURCE = "input_dir"
    CLEANED_IND_CQC_DESTINATION = "output_dir"

    def setUp(self):
        self.merge_ind_cqc_test_df = Mock(name="merge_ind_cqc_data")


class MainTests(CleanIndFilledPostsTests):
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.calculate_care_home_status_count")
    # @patch(f"{PATCH_PATH}.clean_capacity_tracker_non_res_outliers")
    @patch(f"{PATCH_PATH}.clean_capacity_tracker_care_home_outliers")
    @patch(f"{PATCH_PATH}.forward_fill_latest_known_value")
    @patch(f"{PATCH_PATH}.clean_ascwds_filled_post_outliers")
    @patch(f"{PATCH_PATH}.cUtils.create_banded_bed_count_column")
    @patch(f"{PATCH_PATH}.cUtils.calculate_filled_posts_per_bed_ratio")
    @patch(f"{PATCH_PATH}.create_column_with_repeated_values_removed")
    @patch(f"{PATCH_PATH}.calculate_ascwds_filled_posts")
    @patch(f"{PATCH_PATH}.populate_missing_care_home_number_of_beds")
    @patch(f"{PATCH_PATH}.replace_zero_beds_with_null")
    @patch(f"{PATCH_PATH}.remove_dual_registration_cqc_care_homes")
    @patch(f"{PATCH_PATH}.calculate_time_registered_for")
    @patch(f"{PATCH_PATH}.calculate_time_since_dormant")
    @patch(f"{PATCH_PATH}.cUtils.reduce_dataset_to_earliest_file_per_month")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main(
        self,
        scan_parquet_mock: Mock,
        reduce_dataset_to_earliest_file_per_month_mock: Mock,
        calculate_time_since_dormant_mock: Mock,
        calculate_time_registered_for_mock: Mock,
        remove_dual_registration_cqc_care_homes_mock: Mock,
        replace_zero_beds_with_null_mock: Mock,
        populate_missing_care_home_number_of_beds_mock: Mock,
        calculate_ascwds_filled_posts_mock: Mock,
        create_column_with_repeated_values_removed_mock: Mock,
        calculate_filled_posts_per_bed_ratio_mock: Mock,
        create_banded_bed_count_column_mock: Mock,
        clean_ascwds_filled_post_outliers_mock: Mock,
        forward_fill_latest_known_value_mock: Mock,
        clean_capacity_tracker_care_home_outliers_mock: Mock,
        # clean_capacity_tracker_non_res_outliers_mock: Mock,
        calculate_care_home_status_count_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        scan_parquet_mock.return_value = self.merge_ind_cqc_test_df

        job.main(
            self.MERGE_IND_CQC_SOURCE,
            self.CLEANED_IND_CQC_DESTINATION,
        )

        reduce_dataset_to_earliest_file_per_month_mock.assert_called_once()
        calculate_time_registered_for_mock.assert_called_once()
        calculate_time_since_dormant_mock.assert_called_once()
        remove_dual_registration_cqc_care_homes_mock.assert_called_once()
        replace_zero_beds_with_null_mock.assert_called_once()
        populate_missing_care_home_number_of_beds_mock.assert_called_once()
        calculate_ascwds_filled_posts_mock.assert_called_once()
        self.assertEqual(create_column_with_repeated_values_removed_mock.call_count, 2)
        self.assertEqual(calculate_filled_posts_per_bed_ratio_mock.call_count, 3)
        create_banded_bed_count_column_mock.assert_called_once()
        self.assertEqual(forward_fill_latest_known_value_mock.call_count, 2)
        clean_ascwds_filled_post_outliers_mock.assert_called_once()
        clean_capacity_tracker_care_home_outliers_mock.assert_called_once()
        # clean_capacity_tracker_non_res_outliers_mock.assert_called_once()
        calculate_care_home_status_count_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.CLEANED_IND_CQC_DESTINATION,
            append=False,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
