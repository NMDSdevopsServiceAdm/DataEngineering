import polars as pl
import polars.testing as pl_testing
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as job
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_data import (
    PivotJobRoleColsToRowsData as Data,
)
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_schemas import (
    PivotJobRoleColsToRowsSchemas as Schemas,
)


class TestPivotJobRoleColsToRows:
    @pytest.mark.parametrize(
        "case",
        [c.as_pytest_param() for c in Data.no_hardcoded_code_count_cases],
    )
    def test_discovers_and_pivots_all_codes_without_hardcoding(self, case):
        input_lf = pl.LazyFrame(case.input_rows, schema=case.input_schema, orient="row")

        returned_lf = job.pivot_job_role_cols_to_rows(input_lf)

        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.expected_schema, orient="row"
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)

    def test_partial_null_metrics_are_preserved_independently_per_code(self):
        input_lf = pl.LazyFrame(
            Data.partial_null_input_rows,
            schema=Schemas.partial_null_input_schema,
            orient="row",
        )

        returned_lf = job.pivot_job_role_cols_to_rows(input_lf)

        expected_lf = pl.LazyFrame(
            Data.partial_null_expected_rows,
            schema=Schemas.expected_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)

    def test_row_with_all_metrics_null_is_kept(self):
        input_lf = pl.LazyFrame(
            Data.all_null_input_rows,
            schema=Schemas.all_null_input_schema,
            orient="row",
        )

        returned_lf = job.pivot_job_role_cols_to_rows(input_lf)

        expected_lf = pl.LazyFrame(
            Data.all_null_expected_rows,
            schema=Schemas.expected_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)

    def test_raises_value_error_when_no_job_role_columns_found(self):
        input_lf = pl.LazyFrame(
            Data.zero_codes_input_rows,
            schema=Schemas.zero_codes_input_schema,
            orient="row",
        )

        with pytest.raises(ValueError):
            job.pivot_job_role_cols_to_rows(input_lf)

    def test_output_is_narrowed_to_grain_and_job_role_metrics_only(self):
        input_lf = pl.LazyFrame(
            Data.column_scope_input_rows,
            schema=Schemas.column_scope_input_schema,
            orient="row",
        )

        returned_lf = job.pivot_job_role_cols_to_rows(input_lf)

        expected_lf = pl.LazyFrame(
            Data.column_scope_expected_rows,
            schema=Schemas.expected_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)
