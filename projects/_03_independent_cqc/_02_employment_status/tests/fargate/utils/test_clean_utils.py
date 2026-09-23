from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_employment_status.fargate.utils.clean_utils as job
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = (
    "projects._03_independent_cqc._02_employment_status.fargate.utils.clean_utils"
)


class TestCreateEmploymentStatusPercentageColumns:
    @patch(f"{PATCH_PATH}.cleaningUtils.percentage_share_horizontal")
    @patch(f"{PATCH_PATH}.cleaningUtils.remove_repeated_values_over_time_as_group")
    def test_calls_dedup_then_percentage_share_with_expected_args(
        self,
        remove_repeated_values_over_time_as_group_mock: Mock,
        percentage_share_horizontal_mock: Mock,
    ):
        input_lf = Mock()

        returned_lf = job.create_employment_status_percentage_columns(input_lf)

        remove_repeated_values_over_time_as_group_mock.assert_called_once_with(
            input_lf,
            columns_to_clean=[
                EmpStatus.permanent_count,
                EmpStatus.temporary_count,
                EmpStatus.bank_or_pool_count,
                EmpStatus.agency_count,
                EmpStatus.other_count,
            ],
            partition_by_columns=[
                IndCQC.location_id,
                IndCQC.published_job_role_label,
            ],
            date_column=IndCQC.cqc_location_import_date,
        )
        percentage_share_horizontal_mock.assert_called_once_with(
            remove_repeated_values_over_time_as_group_mock.return_value,
            columns=[
                EmpStatus.permanent_count_dedup,
                EmpStatus.temporary_count_dedup,
                EmpStatus.bank_or_pool_count_dedup,
                EmpStatus.agency_count_dedup,
                EmpStatus.other_count_dedup,
            ],
            output_columns=[
                EmpStatus.permanent_percentage,
                EmpStatus.temporary_percentage,
                EmpStatus.bank_or_pool_percentage,
                EmpStatus.agency_percentage,
                EmpStatus.other_percentage,
            ],
        )
        assert returned_lf == percentage_share_horizontal_mock.return_value


class TestCopyPercentagesToCleanColumns:
    def test_clean_percentages_copy_originals(self):
        percentages = {
            EmpStatus.permanent_percentage: [0.5, None],
            EmpStatus.temporary_percentage: [0.2, None],
            EmpStatus.bank_or_pool_percentage: [0.15, None],
            EmpStatus.agency_percentage: [0.1, None],
            EmpStatus.other_percentage: [0.05, None],
        }
        clean_percentages = {
            EmpStatus.permanent_percentage_clean: [0.5, None],
            EmpStatus.temporary_percentage_clean: [0.2, None],
            EmpStatus.bank_or_pool_percentage_clean: [0.15, None],
            EmpStatus.agency_percentage_clean: [0.1, None],
            EmpStatus.other_percentage_clean: [0.05, None],
        }
        input_lf = pl.LazyFrame(
            percentages, schema_overrides=dict.fromkeys(percentages, pl.Float32)
        )

        returned_lf = job.copy_percentages_to_clean_columns(input_lf)

        expected_data = {**percentages, **clean_percentages}
        expected_lf = pl.LazyFrame(
            expected_data, schema_overrides=dict.fromkeys(expected_data, pl.Float32)
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
