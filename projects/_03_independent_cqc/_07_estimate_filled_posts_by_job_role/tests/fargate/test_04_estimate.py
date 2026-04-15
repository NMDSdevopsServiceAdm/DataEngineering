import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._04_estimate as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._04_estimate"


class TestMain(unittest.TestCase):
    TEST_IMPUTED_SOURCE = "some/directory"
    TEST_METADATA_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(
        f"{PATCH_PATH}.eUtils.calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles"
    )
    @patch(f"{PATCH_PATH}.eUtils.adjust_managerial_roles")
    @patch(f"{PATCH_PATH}.eUtils.has_rm_in_cqc_rm_name_list_flag")
    @patch(f"{PATCH_PATH}.eUtils.calculate_estimated_filled_posts_by_job_role")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_succeeds(
        self,
        scan_parquet_mock: Mock,
        calculate_estimated_filled_posts_by_job_role_mock: Mock,
        has_rm_in_cqc_rm_name_list_flag_mock: Mock,
        adjust_managerial_roles_mock: Mock,
        calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.TEST_IMPUTED_SOURCE,
            self.TEST_DESTINATION,
        )

        scan_parquet_mock.assert_called_once()
        calculate_estimated_filled_posts_by_job_role_mock.assert_called_once()
        has_rm_in_cqc_rm_name_list_flag_mock.assert_called_once()
        adjust_managerial_roles_mock.assert_called_once()
        calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.TEST_DESTINATION,
            partition_cols=[Keys.year],
            append=False,
        )
