from unittest.mock import Mock, patch

import projects._03_independent_cqc._02_employment_status.fargate.utils.clean_utils as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import (
    SLVEmploymentStatusColumns as SLVEmpStatus,
)
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols

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
                SLVEmpStatus.permanent_count,
                SLVEmpStatus.temporary_count,
                SLVEmpStatus.bank_or_pool_count,
                SLVEmpStatus.agency_count,
                SLVEmpStatus.other_count,
            ],
            partition_by_columns=[
                IndCQC.location_id,
                SLVCols.published_job_role_label,
            ],
            date_column=IndCQC.cqc_location_import_date,
        )
        percentage_share_horizontal_mock.assert_called_once_with(
            remove_repeated_values_over_time_as_group_mock.return_value,
            columns=[
                SLVEmpStatus.permanent_count_dedup,
                SLVEmpStatus.temporary_count_dedup,
                SLVEmpStatus.bank_or_pool_count_dedup,
                SLVEmpStatus.agency_count_dedup,
                SLVEmpStatus.other_count_dedup,
            ],
            output_columns=[
                SLVEmpStatus.permanent_percentage,
                SLVEmpStatus.temporary_percentage,
                SLVEmpStatus.bank_or_pool_percentage,
                SLVEmpStatus.agency_percentage,
                SLVEmpStatus.other_percentage,
            ],
        )
        assert returned_lf == percentage_share_horizontal_mock.return_value
