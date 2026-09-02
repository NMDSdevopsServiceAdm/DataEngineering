from unittest.mock import ANY, Mock, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._02_clean as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._02_clean"


class TestMain:
    MERGED_DATA_SOURCE = "some/source"
    CLEANED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cleanUtils.create_slv_rate_columns")
    @patch(f"{PATCH_PATH}.cUtils.remove_repeated_values_over_time")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        remove_repeated_values_over_time_mock: Mock,
        create_slv_rate_columns_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.MERGED_DATA_SOURCE,
            self.CLEANED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.MERGED_DATA_SOURCE)
        remove_repeated_values_over_time_mock.assert_called_once_with(
            scan_parquet_mock.return_value,
            columns_to_clean=[SLVCols.starters, SLVCols.leavers, SLVCols.vacancies],
            partition_by_columns=[
                IndCQC.location_id,
                SLVCols.published_job_role_label,
            ],
            date_column=IndCQC.cqc_location_import_date,
        )
        create_slv_rate_columns_mock.assert_called_once_with(
            remove_repeated_values_over_time_mock.return_value
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=create_slv_rate_columns_mock.return_value,
            output_path=self.CLEANED_DATA_DESTINATION,
        )
