import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.estimate_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_test_data import (
    TestEstimateUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

METRIC = IndCQC.estimate_filled_posts_by_job_role


class TestAddEstimatedEmploymentStatusColumns:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_estimated_employment_status_columns_test_cases
        ],
    )
    def test_adds_estimated_percentages_counts_and_employees_as_expected(self, case):
        input_schema = {
            col: pl.Float32 if col != METRIC else pl.Float64 for col in case.input_data
        }
        expected_schema = {
            **input_schema,
            **{col: pl.Float32 for col in job.ESTIMATED_PERCENTAGE_COLUMNS},
            **{col: pl.Float64 for col in job.ESTIMATED_COUNT_COLUMNS},
            EmpStatus.estimated_percentage_source: CatColType.EmploymentStatusEstimateSourceEnumType,
            EmpStatus.estimated_employees: pl.Float64,
        }
        input_lf = pl.LazyFrame(case.input_data, schema=input_schema)
        expected_lf = pl.LazyFrame(case.expected_data, schema=expected_schema)

        returned_lf = job.add_estimated_employment_status_columns(input_lf)

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False
        )
