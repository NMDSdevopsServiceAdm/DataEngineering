from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.clean_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_test_data import (
    TestCleanUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = (
    "projects._03_independent_cqc._02_employment_status.fargate.utils.clean_utils"
)

CLEAN_COUNT_COLUMNS = [
    EmpStatus.permanent_count_clean,
    EmpStatus.temporary_count_clean,
    EmpStatus.bank_or_pool_count_clean,
    EmpStatus.agency_count_clean,
    EmpStatus.other_count_clean,
]

PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage,
    EmpStatus.temporary_percentage,
    EmpStatus.bank_or_pool_percentage,
    EmpStatus.agency_percentage,
    EmpStatus.other_percentage,
]


def _schema_overrides(data: dict) -> dict:
    overrides = {column: pl.Int64 for column in CLEAN_COUNT_COLUMNS if column in data}
    overrides.update(
        {column: pl.Float32 for column in PERCENTAGE_COLUMNS if column in data}
    )
    return overrides


def build_input_lf(input_data: dict) -> pl.LazyFrame:
    return pl.LazyFrame(input_data, schema_overrides=_schema_overrides(input_data))


def build_expected_lf(expected_data: dict) -> pl.LazyFrame:
    return pl.LazyFrame(
        expected_data, schema_overrides=_schema_overrides(expected_data)
    ).with_columns(
        pl.col(EmpStatus.filtering_rule).cast(
            CatColType.EmploymentStatusFilteringRuleCatType
        )
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


class TestNullCountsForLowLocationRatio:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.null_counts_for_low_location_ratio_test_cases
        ],
    )
    def test_nulls_clean_counts_only_where_location_ratio_is_too_low(self, case):
        test_lf = build_input_lf(case.input_data)
        expected_lf = build_expected_lf(case.expected_data)

        returned_lf = job.null_counts_for_low_location_ratio(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


class TestNullCountsForLowOrgRatio:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.null_counts_for_low_org_ratio_test_cases
        ],
    )
    def test_nulls_clean_counts_only_where_org_ratio_is_too_low(self, case):
        test_lf = build_input_lf(case.input_data)
        expected_lf = build_expected_lf(case.expected_data)

        returned_lf = job.null_counts_for_low_org_ratio(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )
