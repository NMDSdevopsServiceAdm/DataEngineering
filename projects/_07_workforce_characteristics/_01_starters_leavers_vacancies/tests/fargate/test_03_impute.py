from unittest.mock import Mock, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._03_impute as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._03_impute"


class TestMain:
    CLEANED_DATA_SOURCE = "some/source"
    IMPUTED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.imputeUtils.forward_fill_within_time_limit")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        forward_fill_within_time_limit_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_DATA_SOURCE,
            self.IMPUTED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.CLEANED_DATA_SOURCE)
        forward_fill_within_time_limit_mock.assert_called_once_with(
            scan_parquet_mock.return_value,
            columns_to_fill={
                SLVCols.turnover_rate_dedup: SLVCols.turnover_rate_imputed,
                SLVCols.starter_rate_dedup: SLVCols.starter_rate_imputed,
                SLVCols.vacancy_rate_dedup: SLVCols.vacancy_rate_imputed,
            },
            partition_by_columns=[
                IndCQC.location_id,
                SLVCols.published_job_role_label,
            ],
            date_column=IndCQC.cqc_location_import_date,
            time_limit=job.NumericalValues.forward_fill_time_limit,
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=forward_fill_within_time_limit_mock.return_value,
            output_path=self.IMPUTED_DATA_DESTINATION,
        )
