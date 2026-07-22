import unittest
from unittest.mock import ANY, Mock, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare as job

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare"


class MainTests(unittest.TestCase):
    CLEANED_ASCWDS_WORKPLACE_SOURCE = "some/source"
    PREPARED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.apply_categorical_labels")
    @patch(f"{PATCH_PATH}.pUtils.convert_job_role_strings_to_number_only")
    @patch(f"{PATCH_PATH}.pUtils.pivot_job_role_cols_to_rows")
    @patch(f"{PATCH_PATH}.pUtils.reduce_to_published_roles")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        reduce_to_published_roles: Mock,
        pivot_job_role_cols_to_rows: Mock,
        convert_job_role_strings_to_number_only: Mock,
        apply_categorical_labels: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_ASCWDS_WORKPLACE_SOURCE,
            self.PREPARED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.CLEANED_ASCWDS_WORKPLACE_SOURCE)

        # TODO: Uncomment these assertions when the placeholder functions are implemented
        # reduce_to_published_roles.assert_called_once()
        # pivot_job_role_cols_to_rows.assert_called_once()
        # convert_job_role_strings_to_number_only.assert_called_once()
        # apply_categorical_labels.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.PREPARED_DATA_DESTINATION,
        )
