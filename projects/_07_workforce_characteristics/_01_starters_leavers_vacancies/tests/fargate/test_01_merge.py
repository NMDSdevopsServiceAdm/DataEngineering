from unittest.mock import ANY, Mock, call, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._01_merge as job

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._01_merge"


class TestMain:
    METADATA_SOURCE = "some/source"
    JOB_ROLE_ESTIMATES_SOURCE = "another/source"
    PREPARED_SLV_DATASET_SOURCE = "other/source"
    EMPLOYEE_STATUS_RATES_SOURCE = "employee/status/rates/source"
    MERGED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.mUtils.apply_employment_status_magic_numbers")
    @patch(f"{PATCH_PATH}.mUtils.join_datasets")
    @patch(f"{PATCH_PATH}.mUtils.convert_ascwds_job_role_columns_to_rows")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    @patch(f"{PATCH_PATH}.expr.is_slv_job_role_column")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        is_slv_job_role_column_mock: Mock,
        scan_csv_mock: Mock,
        convert_ascwds_job_role_columns_to_rows_mock: Mock,
        join_datasets_mock: Mock,
        apply_employment_status_magic_numbers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.METADATA_SOURCE,
            self.JOB_ROLE_ESTIMATES_SOURCE,
            self.PREPARED_SLV_DATASET_SOURCE,
            self.EMPLOYEE_STATUS_RATES_SOURCE,
            self.MERGED_DATA_DESTINATION,
        )

        assert len(scan_parquet_mock.call_args_list) == 3

        scan_calls = [
            call(source=self.METADATA_SOURCE, selected_columns=job.metadata_columns),
            call(
                source=self.JOB_ROLE_ESTIMATES_SOURCE,
                selected_columns=job.job_role_estimates_columns,
            ),
            call(self.PREPARED_SLV_DATASET_SOURCE),
        ]
        scan_parquet_mock.assert_has_calls(scan_calls)

        # TODO: Uncomment these assertions when the placeholder functions are implemented
        is_slv_job_role_column_mock.assert_called_once()
        # join_datasets_mock.assert_called_once()
        # apply_employment_status_magic_numbers_mock.assert_called_once()

        scan_csv_mock.assert_called_once_with(
            self.EMPLOYEE_STATUS_RATES_SOURCE, schema=ANY
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.MERGED_DATA_DESTINATION,
        )
