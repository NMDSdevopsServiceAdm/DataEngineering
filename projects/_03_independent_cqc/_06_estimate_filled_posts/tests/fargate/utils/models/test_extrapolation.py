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


# # TODO: change to call mocking and call checks
# class MainTests:
#     def setUp(self) -> None:
#         self.test_lf = pl.LazyFrame(
#             Data.extrapolation_rows, Schemas.extrapolation_schema, orient="row"
#         )
#         self.returned_lf = job.model_extrapolation(
#             self.test_lf,
#             column_with_null_values=IndCQC.ascwds_pir_merged,
#             model_to_extrapolate_from=IndCQC.posts_rolling_average_model,
#             extrapolation_method="nominal",
#         )
#         self.returned_lf.show(12)
#         self.expected_lf = self.test_lf.drop()

#     @pytest.mark.skip(reason="todo")
#     def test_model_extrapolation_row_count_unchanged(self):
#         pl_testing.assert_frame_equal(
#             self.returned_lf.count(), self.expected_lf.count()
#         )


# # class DefineWindowSpecsTests(ModelExtrapolationTests):
# #     def test_define_window_spec_return_type(self):
# #         returned_window_specs = job.define_window_specs()
# #         self.assertIsInstance(returned_window_specs, tuple)
# #         self.assertEqual(len(returned_window_specs), 2)
# #         self.assertIsInstance(returned_window_specs[0], WindowSpec)
# #         self.assertIsInstance(returned_window_specs[1], WindowSpec)


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


# # TODO
# class CombineExtrapolationTests(ModelExtrapolationTests):
#     def setUp(self):
#         super().setUp()

#         test_combine_extrapolation_lf = self.spark.createDataFrame(
#             Data.combine_extrapolation_rows,
#             Schemas.combine_extrapolation_schema,
#         )
#         self.returned_lf = job.combine_extrapolation(test_combine_extrapolation_lf)
#         self.expected_lf = self.spark.createDataFrame(
#             Data.expected_combine_extrapolation_rows,
#             Schemas.expected_combine_extrapolation_schema,
#         )
#         self.returned_data = self.returned_lf.sort(
#             IndCQC.location_id, IndCQC.cqc_location_import_date
#         ).collect()
#         self.expected_data = self.expected_lf.collect()

#     @pytest.mark.skip(reason="todo")
#     def test_combine_extrapolation_returns_expected_columns(self):
#         self.assertTrue(self.returned_lf.columns, self.expected_lf.columns)

#     @pytest.mark.skip(reason="todo")
#     def test_combine_extrapolation_returns_expected_values(self):
#         for i in range(len(self.returned_data)):
#             self.assertEqual(
#                 self.returned_data[i][IndCQC.extrapolation_model],
#                 self.expected_data[i][IndCQC.extrapolation_model],
#                 f"Returned value in row {i} does not match expected",
#             )
