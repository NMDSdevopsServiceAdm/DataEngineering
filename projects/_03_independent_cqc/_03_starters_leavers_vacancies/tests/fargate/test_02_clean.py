from unittest.mock import Mock, call, patch

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._02_clean as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)
from utils.column_values.categorical_column_values import SLVFilteringRule

PATCH_PATH = (
    "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._02_clean"
)


class TestMain:
    MERGED_DATA_SOURCE = "some/source"
    CLEANED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cleanUtils.create_slv_rate_columns")
    @patch(f"{PATCH_PATH}.cleaningUtils.remove_repeated_values_over_time_as_group")
    @patch(f"{PATCH_PATH}.cUtils.null_not_known_values")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        null_not_known_values_mock: Mock,
        remove_repeated_values_over_time_as_group_mock: Mock,
        create_slv_rate_columns_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.MERGED_DATA_SOURCE,
            self.CLEANED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.MERGED_DATA_SOURCE)
        null_not_known_values_mock.assert_called_once_with(
            scan_parquet_mock.return_value.with_columns.return_value,
            columns_to_clean=[SLVCols.starters, SLVCols.leavers, SLVCols.vacancies],
            not_known_code=job.NOT_KNOWN_CODE,
            populated_rule=SLVFilteringRule.populated,
            missing_rule=SLVFilteringRule.missing_data,
            not_known_rule=SLVFilteringRule.contained_invalid_missing_data_code,
        )
        partition_by_columns = [IndCQC.location_id, IndCQC.published_job_role_label]
        workplace_columns = [IndCQC.location_id, IndCQC.cqc_location_import_date]
        remove_repeated_values_over_time_as_group_mock.assert_has_calls(
            [
                call(
                    null_not_known_values_mock.return_value,
                    columns_to_clean=[SLVCols.starters_cleaned],
                    partition_by_columns=partition_by_columns,
                    date_column=IndCQC.cqc_location_import_date,
                    workplace_columns=workplace_columns,
                ),
                call(
                    remove_repeated_values_over_time_as_group_mock.return_value,
                    columns_to_clean=[SLVCols.leavers_cleaned],
                    partition_by_columns=partition_by_columns,
                    date_column=IndCQC.cqc_location_import_date,
                    workplace_columns=workplace_columns,
                ),
                call(
                    remove_repeated_values_over_time_as_group_mock.return_value,
                    columns_to_clean=[SLVCols.vacancies_cleaned],
                    partition_by_columns=partition_by_columns,
                    date_column=IndCQC.cqc_location_import_date,
                    workplace_columns=workplace_columns,
                ),
            ]
        )
        assert remove_repeated_values_over_time_as_group_mock.call_count == 3
        create_slv_rate_columns_mock.assert_called_once_with(
            remove_repeated_values_over_time_as_group_mock.return_value
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=create_slv_rate_columns_mock.return_value,
            output_path=self.CLEANED_DATA_DESTINATION,
        )
