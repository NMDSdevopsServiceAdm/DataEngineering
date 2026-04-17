from unittest.mock import MagicMock

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.interpolation as job
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


class TestCalculateProportionOfTimeBetweenSubmissions:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.calculate_time_between_submissions_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        expected_lf = pl.LazyFrame(
            case.data,
            schema=Schemas.time_between_submissions_schema,
            orient="row",
        )

        input_lf = expected_lf.drop(
            IndCqc.time_between_submissions,
            IndCqc.proportion_of_time_between_submissions,
        )
        returned_lf = job.calculate_proportion_of_time_between_submissions(
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
