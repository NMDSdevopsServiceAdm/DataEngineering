import warnings
from datetime import date
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.extrapolation as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ModelExtrapolation as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ModelExtrapolation as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.extrapolation"


class TestModelExtrapolation:
    mock_input_lf = pl.LazyFrame(schema=Schemas.extrapolation_schema)

    @patch(f"{PATCH_PATH}.combine_extrapolation")
    @patch(f"{PATCH_PATH}.extrapolation_backwards")
    @patch(f"{PATCH_PATH}.extrapolation_forwards")
    @patch(
        f"{PATCH_PATH}.calculate_first_and_final_submission_dates",
    )
    def test_model_extrapolation_calls_required_functions(
        self,
        mock_calculate_first_and_final_submission_dates: Mock,
        mock_extrapolation_forwards: Mock,
        mock_extrapolation_backwards: Mock,
        mock_combine_extrapolation: Mock,
    ):
        job.model_extrapolation(
            self.mock_input_lf,
            column_with_null_values=IndCQC.ascwds_pir_merged,
            model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
            extrapolation_method="nominal",
        )

        mock_calculate_first_and_final_submission_dates.assert_called_once()
        mock_extrapolation_forwards.assert_called_once()
        mock_extrapolation_backwards.assert_called_once()
        mock_combine_extrapolation.assert_called_once()

    def test_model_extrpolation_returns_expected_results(self):
        expected_lf = pl.LazyFrame(
            data=Data.extrapolation_rows,
            schema=Schemas.extrapolation_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(IndCQC.extrapolation_model)
        returned_lf = job.model_extrapolation(
            input_lf,
            column_with_null_values=IndCQC.ascwds_pir_merged,
            model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
            extrapolation_method="nominal",
        ).drop(IndCQC.extrapolation_forwards, IndCQC.extrapolation_backwards)
        returned_lf.show(20)
        pl_testing.assert_frame_equal(expected_lf, returned_lf, abs_tol=0.00001)


class TestCalculateFirstAndLastSubmissionDates:
    expected_lf = pl.LazyFrame(
        Data.expected_first_and_last_submission_dates_rows,
        Schemas.expected_first_and_final_submission_dates_schema,
        orient="row",
    )
    input_lf = expected_lf.drop(
        IndCQC.first_submission_time, IndCQC.final_submission_time
    )
    returned_lf = job.calculate_first_and_final_submission_dates(
        input_lf,
        column_with_null_values=IndCQC.ascwds_pir_merged,
    )

    def test_calculate_first_and_final_submission_dates_returns_expected_values(
        self,
    ):
        pl_testing.assert_frame_equal(
            self.expected_lf, self.returned_lf, abs_tol=0.00001
        )


class TestExtrapolationForwardsWhenNominal:
    @pytest.mark.parametrize(
        "extrapolation_forwards_when_nominal_data",
        [
            case.as_pytest_param()
            for case in Data.extrapolation_forwards_when_nominal_test_cases
        ],
    )
    def test_returned_extrapolation_forwards_values_match_expected_when_nominal(
        self,
        extrapolation_forwards_when_nominal_data,
    ):
        expected_nominal_lf = pl.LazyFrame(
            extrapolation_forwards_when_nominal_data,
            Schemas.expected_extrapolation_forwards_schema,
            orient="row",
        )
        input_lf = expected_nominal_lf.drop(
            IndCQC.extrapolation_forwards,
        )
        returned_nominal_lf = job.extrapolation_forwards(
            input_lf,
            column_with_null_values=IndCQC.ascwds_pir_merged,
            model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
            extrapolation_method="nominal",
        )
        returned_nominal_lf.show(10)
        expected_nominal_lf.show(10)

        pl_testing.assert_frame_equal(
            returned_nominal_lf,
            expected_nominal_lf,
            abs_tol=0.00001,
            check_row_order=False,
        )


class TestExtrapolationForwardsWhenRatio:
    @pytest.mark.parametrize(
        "extrapolation_forwards_when_ratio_data",
        [
            case.as_pytest_param()
            for case in Data.extrapolation_forwards_when_ratio_test_cases
        ],
    )
    def test_returned_extrapolation_forwards_values_match_expected_when_ratio(
        self,
        extrapolation_forwards_when_ratio_data,
    ):
        expected_ratio_lf = pl.LazyFrame(
            extrapolation_forwards_when_ratio_data,
            Schemas.expected_extrapolation_forwards_schema,
            orient="row",
        )
        input_lf = expected_ratio_lf.drop(
            IndCQC.extrapolation_forwards,
        )
        returned_ratio_lf = job.extrapolation_forwards(
            input_lf,
            column_with_null_values=IndCQC.ascwds_pir_merged,
            model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
            extrapolation_method="ratio",
        )
        returned_ratio_lf.show(10)
        expected_ratio_lf.show(10)

        pl_testing.assert_frame_equal(
            returned_ratio_lf,
            expected_ratio_lf,
            abs_tol=0.00001,
            check_row_order=False,
        )


class TestExtrapolationForwardsWhenInvalidMethod:
    def test_error_raised_for_invalid_extrapolation_method(self):
        input_lf = pl.LazyFrame(
            Data.expected_extrapolation_forwards_when_error_rows,
            Schemas.expected_extrapolation_forwards_schema,
            orient="row",
        ).drop(IndCQC.extrapolation_forwards)
        expected_error_message = "Error: method must be either 'ratio' or 'nominal'."
        with pytest.raises(ValueError, match=expected_error_message):
            job.extrapolation_forwards(
                input_lf,
                column_with_null_values=IndCQC.ascwds_pir_merged,
                model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
                extrapolation_method="other",  # Invalid method
            )


class TestExtrapolationBackwardsWhenNominal:
    @pytest.mark.parametrize(
        "extrapolation_backwards_when_nominal_data",
        [
            case.as_pytest_param()
            for case in Data.extrapolation_backwards_when_nominal_test_cases
        ],
    )
    def test_returned_extrapolation_backwards_values_match_expected_when_nominal(
        self,
        extrapolation_backwards_when_nominal_data,
    ):
        expected_nominal_lf = pl.LazyFrame(
            extrapolation_backwards_when_nominal_data,
            Schemas.expected_extrapolation_backwards_schema,
            orient="row",
        )
        input_lf = expected_nominal_lf.drop(
            IndCQC.extrapolation_backwards,
        )
        returned_nominal_lf = job.extrapolation_backwards(
            input_lf,
            column_with_null_values=IndCQC.ascwds_pir_merged,
            model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
            extrapolation_method="nominal",
        )
        returned_nominal_lf.show(10)
        expected_nominal_lf.show(10)

        pl_testing.assert_frame_equal(
            returned_nominal_lf,
            expected_nominal_lf,
            abs_tol=0.00001,
            check_row_order=False,
        )


class TestExtrapolationBackwardsWhenRatio:
    @pytest.mark.parametrize(
        "extrapolation_backwards_when_ratio_data",
        [
            case.as_pytest_param()
            for case in Data.extrapolation_backwards_when_ratio_test_cases
        ],
    )
    def test_returned_extrapolation_backwards_values_match_expected_when_ratio(
        self,
        extrapolation_backwards_when_ratio_data,
    ):
        expected_ratio_lf = pl.LazyFrame(
            extrapolation_backwards_when_ratio_data,
            Schemas.expected_extrapolation_backwards_schema,
            orient="row",
        )
        input_lf = expected_ratio_lf.drop(
            IndCQC.extrapolation_backwards,
        )
        returned_ratio_lf = job.extrapolation_backwards(
            input_lf,
            column_with_null_values=IndCQC.ascwds_pir_merged,
            model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
            extrapolation_method="ratio",
        )
        returned_ratio_lf.show(10)
        expected_ratio_lf.show(10)

        pl_testing.assert_frame_equal(
            returned_ratio_lf,
            expected_ratio_lf,
            abs_tol=0.00001,
            check_row_order=False,
        )


class TestExtrapolationBackwardsWhenInvalidMethod:
    def test_error_raised_for_invalid_extrapolation_method(self):
        input_lf = pl.LazyFrame(
            Data.expected_extrapolation_backwards_when_error_rows,
            Schemas.expected_extrapolation_backwards_schema,
            orient="row",
        ).drop(IndCQC.extrapolation_backwards)
        expected_error_message = "Error: method must be either 'ratio' or 'nominal'."
        with pytest.raises(ValueError, match=expected_error_message):
            job.extrapolation_backwards(
                input_lf,
                column_with_null_values=IndCQC.ascwds_pir_merged,
                model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
                extrapolation_method="other",  # Invalid method
            )


class TestCombineExtrapolation:
    @pytest.mark.parametrize(
        "combine_extrapolation_data",
        [case.as_pytest_param() for case in Data.combine_extrapolation_test_cases],
    )
    def test_combine_extrapolation_returns_expected_data(
        self, combine_extrapolation_data
    ):
        expected_lf = pl.LazyFrame(
            combine_extrapolation_data,
            Schemas.expected_combine_extrapolation_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(IndCQC.extrapolation_model)
        returned_lf = job.combine_extrapolation(
            input_lf,
        )
        expected_lf.show(10)
        returned_lf.show(10)
        pl_testing.assert_frame_equal(expected_lf, returned_lf, abs_tol=0.00001)
