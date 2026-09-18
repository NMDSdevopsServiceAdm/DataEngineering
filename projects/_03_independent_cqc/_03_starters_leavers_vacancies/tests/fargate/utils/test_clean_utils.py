import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.utils.clean_utils as job
from projects._03_independent_cqc._03_starters_leavers_vacancies.unittest_data.polars_slv_test_data import (
    TestCleanUtilsData as Data,
)
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols

INPUT_SCHEMA_OVERRIDES = {
    SLVCols.employees: pl.Int16,
    SLVCols.starters: pl.Int16,
    SLVCols.leavers: pl.Int16,
    SLVCols.vacancies: pl.Int16,
}

EXPECTED_SCHEMA_OVERRIDES = {
    **INPUT_SCHEMA_OVERRIDES,
    SLVCols.turnover_rate: pl.Float32,
    SLVCols.starter_rate: pl.Float32,
    SLVCols.vacancy_rate: pl.Float32,
}

SLV_SCHEMA_OVERRIDES = {
    SLVCols.starters: pl.Int16,
    SLVCols.leavers: pl.Int16,
    SLVCols.vacancies: pl.Int16,
}


class TestNullNotKnownSlvValues:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.null_not_known_slv_values_test_cases
        ],
    )
    def test_returns_expected_values(self, case):
        test_lf = pl.LazyFrame(case.input_data, schema_overrides=SLV_SCHEMA_OVERRIDES)
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=SLV_SCHEMA_OVERRIDES
        )

        returned_lf = job.null_not_known_slv_values(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestCreateSlvRateColumns:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.create_slv_rate_columns_test_cases
        ],
    )
    def test_returns_expected_rate_values(self, case):
        test_lf = pl.LazyFrame(case.input_data, schema_overrides=INPUT_SCHEMA_OVERRIDES)
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=EXPECTED_SCHEMA_OVERRIDES
        )

        returned_lf = job.create_slv_rate_columns(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )
