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


CLEAN_COUNT_COLUMNS = [
    EmpStatus.permanent_count_clean,
    EmpStatus.temporary_count_clean,
    EmpStatus.bank_or_pool_count_clean,
    EmpStatus.agency_count_clean,
    EmpStatus.other_count_clean,
]


def build_expected_lf(expected_data: dict) -> pl.LazyFrame:
    return pl.LazyFrame(
        expected_data,
        schema_overrides={
            column: pl.Int64
            for column in CLEAN_COUNT_COLUMNS
            if column in expected_data
        },
    ).with_columns(
        pl.col(EmpStatus.filtering_rule).cast(
            CatColType.EmploymentStatusFilteringRuleCatType
        )
    )


class TestSeedEmploymentStatusCleanColumns:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.seed_employment_status_clean_columns_test_cases
        ],
    )
    def test_copies_raw_counts_and_flags_populated_vs_missing(self, case):
        test_lf = pl.LazyFrame(case.input_data)
        expected_lf = build_expected_lf(case.expected_data)

        returned_lf = job.seed_employment_status_clean_columns(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


class TestNullEmploymentStatusCountsWhereLocationPermanentTemporaryRatioIsTooLow:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.null_employment_status_counts_where_location_permanent_temporary_ratio_is_too_low_test_cases
        ],
    )
    def test_nulls_clean_counts_only_where_location_ratio_is_too_low(self, case):
        test_lf = pl.LazyFrame(case.input_data)
        expected_lf = build_expected_lf(case.expected_data)

        returned_lf = job.null_employment_status_counts_where_location_permanent_temporary_ratio_is_too_low(
            test_lf
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


class TestNullEmploymentStatusCountsWhereOrgPermanentTemporaryRatioIsTooLow:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.null_employment_status_counts_where_org_permanent_temporary_ratio_is_too_low_test_cases
        ],
    )
    def test_nulls_clean_counts_only_where_org_ratio_is_too_low(self, case):
        test_lf = pl.LazyFrame(case.input_data)
        expected_lf = build_expected_lf(case.expected_data)

        returned_lf = job.null_employment_status_counts_where_org_permanent_temporary_ratio_is_too_low(
            test_lf
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )
