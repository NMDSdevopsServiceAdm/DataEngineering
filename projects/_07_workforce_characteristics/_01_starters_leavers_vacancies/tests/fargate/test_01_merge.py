from unittest.mock import ANY, Mock, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._01_merge as job

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._01_merge"


class TestMain:
    METADATA_SOURCE = "some/source"
    JOB_ROLE_ESTIMATES_SOURCE = "another/source"
    PREPARED_SLV_DATASET_SOURCE = "other/source"
    EMPLOYMENT_STATUS_RATES_SOURCE = "employment/status/rates/source"
    MERGED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.mUtils.apply_employment_status_magic_numbers")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    @patch(f"{PATCH_PATH}.mUtils.collapse_job_role_estimates_to_published_labels")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        collapse_job_role_estimates_to_published_labels_mock: Mock,
        scan_csv_mock: Mock,
        apply_employment_status_magic_numbers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.METADATA_SOURCE,
            self.JOB_ROLE_ESTIMATES_SOURCE,
            self.PREPARED_SLV_DATASET_SOURCE,
            self.EMPLOYMENT_STATUS_RATES_SOURCE,
            self.MERGED_DATA_DESTINATION,
        )

        assert len(scan_parquet_mock.call_args_list) == 3

        scan_parquet_mock.assert_any_call(
            source=self.METADATA_SOURCE, selected_columns=job.metadata_columns
        )
        scan_parquet_mock.assert_any_call(
            source=self.JOB_ROLE_ESTIMATES_SOURCE,
            selected_columns=job.job_role_estimates_columns,
        )
        scan_parquet_mock.assert_any_call(
            self.PREPARED_SLV_DATASET_SOURCE, selected_columns=job.workplace_columns
        )

        collapse_job_role_estimates_to_published_labels_mock.assert_called_once()

        # TODO: Uncomment when the placeholder function is implemented
        # apply_employment_status_magic_numbers_mock.assert_called_once()

        scan_csv_mock.assert_called_once_with(
            self.EMPLOYMENT_STATUS_RATES_SOURCE, schema=ANY
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.MERGED_DATA_DESTINATION,
        )
