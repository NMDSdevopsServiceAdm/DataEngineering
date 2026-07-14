import unittest
from unittest.mock import ANY, Mock, call, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._01_merge as job

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._01_merge"


class MainTests(unittest.TestCase):
    METADATA_SOURCE = "some/source"
    JOB_ROLE_ESTIMATES_SOURCE = "another/source"
    CLEANED_ASCWDS_WORKPLACE_SOURCE = "other/source"
    MERGED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.mUtils.apply_employment_status_magic_numbers")
    @patch(f"{PATCH_PATH}.mUtils.join_datasets")
    @patch(f"{PATCH_PATH}.mUtils.convert_ascwds_job_role_columns_to_rows")
    @patch(f"{PATCH_PATH}.mUtils.slv_cols_selector")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        slv_cols_selector_mock: Mock,
        convert_ascwds_job_role_columns_to_rows_mock: Mock,
        join_datasets_mock: Mock,
        apply_employment_status_magic_numbers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.METADATA_SOURCE,
            self.JOB_ROLE_ESTIMATES_SOURCE,
            self.CLEANED_ASCWDS_WORKPLACE_SOURCE,
            self.MERGED_DATA_DESTINATION,
        )

        assert len(scan_parquet_mock.call_args_list) == 3

        scan_calls = [
            call(source=self.METADATA_SOURCE, selected_columns=job.metadata_columns),
            call(self.JOB_ROLE_ESTIMATES_SOURCE),
            call(self.CLEANED_ASCWDS_WORKPLACE_SOURCE),
        ]
        scan_parquet_mock.assert_has_calls(scan_calls)

        # TODO: Uncomment these assertions when the placeholder functions are implemented
        slv_cols_selector_mock.assert_called_once()
        # convert_ascwds_job_role_columns_to_rows_mock.assert_called_once()
        # join_datasets_mock.assert_called_once()
        # apply_employment_status_magic_numbers_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.MERGED_DATA_DESTINATION,
        )
