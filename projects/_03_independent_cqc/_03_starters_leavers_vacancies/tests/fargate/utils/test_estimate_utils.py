import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.utils.estimate_utils as job
from projects._03_independent_cqc._03_starters_leavers_vacancies.unittest_data.polars_slv_test_data import (
    TestEstimateData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

INPUT_SCHEMA_OVERRIDES = {
    EmpStatus.estimated_employees: pl.Float64,
    SLVCols.estimated_starter_rate: pl.Float32,
    SLVCols.estimated_turnover_rate: pl.Float32,
    SLVCols.estimated_vacancy_rate: pl.Float32,
}

EXPECTED_SCHEMA_OVERRIDES = {
    **INPUT_SCHEMA_OVERRIDES,
    SLVCols.estimated_starters: pl.Float64,
    SLVCols.estimated_leavers: pl.Float64,
    SLVCols.estimated_vacancies: pl.Float64,
}


class TestAddEstimatedSlvCounts:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_estimated_slv_counts_test_cases
        ],
    )
    def test_returns_expected_counts(self, case):
        test_lf = pl.LazyFrame(case.input_data, schema_overrides=INPUT_SCHEMA_OVERRIDES)
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=EXPECTED_SCHEMA_OVERRIDES
        )

        returned_lf = job.add_estimated_slv_counts(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
