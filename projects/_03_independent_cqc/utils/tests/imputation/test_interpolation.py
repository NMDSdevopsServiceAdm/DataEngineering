from datetime import date
from unittest.mock import MagicMock

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc.utils.imputation.interpolation as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    InterpolationData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    InterpolationSchema as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

PATCH_PATH: str = (
    "projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.interpolation"
)


class TestModelInterpolation:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.interpolation_test_cases],
    )
    def test_function_returns_expected_values(self, case):
        expected_lf = pl.LazyFrame(
            case.data,
            schema=Schemas.interpolation_schema,
            orient="row",
        )

        self.input_lf = expected_lf.drop(IndCqc.interpolation_model)
        returned_lf = job.model_interpolation(
            self.input_lf,
            IndCqc.ascwds_pir_merged,
            method=case.method,
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_main_raises_error_if_method_not_a_valid_option(self):
        mock_input_lf = MagicMock(name="input_lf")
        with pytest.raises(ValueError) as context:
            job.model_interpolation(
                mock_input_lf,
                IndCqc.ascwds_pir_merged,
                "invalid",
            )

        assert "Error: method must be either 'straight' or 'trend'" in str(
            context.value
        )


class TestCalculateResiduals:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.calculate_residual_test_cases],
    )
    def test_function_returns_expected_values(self, case):
        expected_lf = pl.LazyFrame(
            case.data,
            schema=Schemas.calculate_residual_schema,
            orient="row",
        )

        input_lf = expected_lf.drop(IndCqc.residual)
        returned_lf = job.calculate_residuals(
            input_lf,
            IndCqc.ascwds_pir_merged,
            IndCqc.extrapolation_forwards,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestCalculateResidualsGroupColumns:
    row_id = "row_id"
    group_col = "group_col"
    first_col = "first_col"
    second_col = "second_col"

    input_lf = pl.LazyFrame(
        {
            row_id: [1, 2, 3, 4],
            IndCqc.location_id: ["1-001"] * 4,
            IndCqc.cqc_location_import_date: [
                date(2023, 1, 1),
                date(2023, 2, 1),
                date(2023, 3, 1),
                date(2023, 4, 1),
            ],
            group_col: ["A", "A", "B", "B"],
            first_col: [None, None, 10.0, None],
            second_col: [None, None, 5.0, None],
        }
    )

    def test_default_group_columns_backfills_residual_over_locationid(
        self,
    ):
        returned_lf = job.calculate_residuals(
            self.input_lf, self.first_col, self.second_col
        )

        expected_lf = pl.LazyFrame(
            {self.row_id: [1, 2, 3, 4], IndCqc.residual: [5.0, 5.0, 5.0, None]}
        )

        pl_testing.assert_frame_equal(
            returned_lf.select(self.row_id, IndCqc.residual), expected_lf
        )

    def test_group_columns_parameter_backfills_residual_over_given_groups(
        self,
    ):
        returned_lf = job.calculate_residuals(
            self.input_lf,
            self.first_col,
            self.second_col,
            group_columns=[IndCqc.location_id, self.group_col],
        )

        expected_lf = pl.LazyFrame(
            {self.row_id: [1, 2, 3, 4], IndCqc.residual: [None, None, 5.0, None]}
        )

        pl_testing.assert_frame_equal(
            returned_lf.select(self.row_id, IndCqc.residual), expected_lf
        )


class TestCalculateProportionOfTimeBetweenSubmissions:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.calculate_days_between_submissions_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        expected_lf = pl.LazyFrame(
            case.data,
            schema=Schemas.days_between_submissions_schema,
            orient="row",
        )

        input_lf = expected_lf.drop(
            IndCqc.days_between_submissions,
            IndCqc.proportion_of_days_between_submissions,
        )
        returned_lf = job.calculate_proportion_of_days_between_submissions(
            input_lf, IndCqc.ascwds_pir_merged
        )
        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_row_order=False, abs_tol=1e-2
        )


class TestCalculateInterpolatedValues:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.calculate_interpolated_values_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        expected_lf = pl.LazyFrame(
            case.data,
            schema=Schemas.calculate_interpolated_values_schema,
            orient="row",
        )

        input_lf = expected_lf.drop(IndCqc.interpolation_model)
        returned_lf = job.calculate_interpolated_values(
            input_lf,
            IndCqc.previous_non_null_value,
            IndCqc.interpolation_model,
            max_days_between_submissions=case.max_days_between_submissions,
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
