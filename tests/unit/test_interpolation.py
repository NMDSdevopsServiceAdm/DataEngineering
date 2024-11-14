import unittest
from unittest.mock import ANY, Mock, patch
import warnings

from pyspark.sql import Window, WindowSpec

import utils.estimate_filled_posts.models.interpolation as job
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelInterpolation as Data
from tests.test_file_schemas import ModelInterpolation as Schemas


class ModelInterpolationTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.interpolation_df = self.spark.createDataFrame(
            Data.interpolation_rows, Schemas.interpolation_schema
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(ModelInterpolationTests):
    def setUp(self) -> None:
        super().setUp()

        self.returned_df = job.model_interpolation(
            self.interpolation_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            "trend",
        )

    def test_model_interpolation_row_count_unchanged(self):
        self.assertEqual(self.returned_df.count(), self.interpolation_df.count())

    def test_model_interpolation_returns_new_column(self):
        self.assertIn(IndCqc.interpolation_model, self.returned_df.columns)

    def test_main_doesnt_raise_error_if_method_is_a_valid_option(self):
        try:
            job.model_interpolation(
                self.interpolation_df,
                IndCqc.ascwds_filled_posts_dedup_clean,
                "straight",
            )

        except ValueError:
            self.fail("Chosen 'method' raised ValueError unexpectedly")

    def test_main_raises_error_if_method_not_a_valid_option(self):
        with self.assertRaises(ValueError) as context:
            job.model_interpolation(
                self.interpolation_df,
                IndCqc.ascwds_filled_posts_dedup_clean,
                "invalid",
            )

        self.assertIn(
            "Error: method must be either 'straight' or 'trend'", str(context.exception)
        )


class DefineWindowSpecsTests(ModelInterpolationTests):
    def setUp(self) -> None:
        super().setUp()

    def test_define_window_spec_return_type(self):
        returned_window_specs = job.define_window_specs()
        self.assertIsInstance(returned_window_specs, tuple)
        self.assertEqual(len(returned_window_specs), 2)
        self.assertIsInstance(returned_window_specs[0], WindowSpec)
        self.assertIsInstance(returned_window_specs[1], WindowSpec)


class CalculateResidualsTests(ModelInterpolationTests):
    def setUp(self):
        super().setUp()

        self.window_spec: Window = (
            Window.partitionBy(IndCqc.location_id)
            .orderBy(IndCqc.unix_time)
            .rowsBetween(Window.currentRow, Window.unboundedFollowing)
        )
        self.test_df = self.spark.createDataFrame(
            Data.calculate_residual_returns_none_when_extrapolation_forwards_is_none_rows,
            Schemas.calculate_residual_schema,
        )
        self.returned_df = job.calculate_residuals(
            self.test_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            self.window_spec,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_residual_returns_none_when_extrapolation_forwards_is_none_rows,
            Schemas.expected_calculate_residual_schema,
        )

    def test_calculate_residuals_returns_expected_columns(self):
        self.assertTrue(self.returned_df.columns, self.expected_df.columns)

    def test_calculate_residuals_returns_none_when_extrapolation_forwards_is_none(
        self,
    ):
        returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        expected_data = self.expected_df.collect()
        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i][IndCqc.extrapolation_residual],
                expected_data[i][IndCqc.extrapolation_residual],
                f"Returned value in row {i} does not match expected",
            )

    def test_calculate_residuals_returns_expected_values_when_extrapolation_forwards_is_known(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_residual_returns_expected_values_when_extrapolation_forwards_is_known_rows,
            Schemas.calculate_residual_schema,
        )
        returned_df = job.calculate_residuals(
            test_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            self.window_spec,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_residual_returns_expected_values_when_extrapolation_forwards_is_known_rows,
            Schemas.expected_calculate_residual_schema,
        )
        returned_data = returned_df.sort(IndCqc.location_id, IndCqc.unix_time).collect()
        expected_data = expected_df.collect()
        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i][IndCqc.extrapolation_residual],
                expected_data[i][IndCqc.extrapolation_residual],
                f"Returned value in row {i} does not match expected",
            )

    def test_calculate_residuals_returns_none_date_after_final_non_null_submission(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_residual_returns_none_date_after_final_non_null_submission_rows,
            Schemas.calculate_residual_schema,
        )
        returned_df = job.calculate_residuals(
            test_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            self.window_spec,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_residual_returns_none_date_after_final_non_null_submission_rows,
            Schemas.expected_calculate_residual_schema,
        )
        returned_data = returned_df.sort(IndCqc.location_id, IndCqc.unix_time).collect()
        expected_data = expected_df.collect()
        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i][IndCqc.extrapolation_residual],
                expected_data[i][IndCqc.extrapolation_residual],
                f"Returned value in row {i} does not match expected",
            )


class CalculateProportionOfTimeBetweenSubmissionsTests(ModelInterpolationTests):
    def setUp(self) -> None:
        super().setUp()

        self.column_with_null_values = IndCqc.ascwds_filled_posts_dedup_clean
        self.window_spec_backwards = (
            Window.partitionBy(IndCqc.location_id)
            .orderBy(IndCqc.unix_time)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        self.window_spec_forward = (
            Window.partitionBy(IndCqc.location_id)
            .orderBy(IndCqc.unix_time)
            .rowsBetween(Window.currentRow, Window.unboundedFollowing)
        )
        self.input_df = self.spark.createDataFrame(
            Data.time_between_submissions_rows,
            Schemas.time_between_submissions_schema,
        )
        self.mock_df = self.spark.createDataFrame(
            Data.time_between_submissions_mock_rows,
            Schemas.time_between_submissions_mock_schema,
        )
        self.returned_df = job.calculate_proportion_of_time_between_submissions(
            self.input_df,
            self.column_with_null_values,
            self.window_spec_backwards,
            self.window_spec_forward,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_time_between_submissions_rows,
            Schemas.expected_time_between_submissions_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    @patch("utils.estimate_filled_posts.models.interpolation.get_selected_value")
    def test_calculate_proportion_of_time_between_submissions_calls_correct_functions(
        self,
        get_selected_value_mock: Mock,
    ):
        get_selected_value_mock.return_value = self.mock_df

        job.calculate_proportion_of_time_between_submissions(
            self.input_df,
            self.column_with_null_values,
            self.window_spec_backwards,
            self.window_spec_forward,
        )

        self.assertEqual(get_selected_value_mock.call_count, 2)

        get_selected_value_mock.assert_any_call(
            self.input_df,
            self.window_spec_backwards,
            self.column_with_null_values,
            IndCqc.unix_time,
            IndCqc.previous_submission_time,
            "last",
        )
        get_selected_value_mock.assert_any_call(
            ANY,
            self.window_spec_forward,
            self.column_with_null_values,
            IndCqc.unix_time,
            IndCqc.next_submission_time,
            "first",
        )

    def test_calculate_proportion_of_time_between_submissions_returns_same_number_of_rows(
        self,
    ):
        self.assertEqual(self.input_df.count(), self.returned_df.count())

    def test_proportion_of_time_between_submissions_added_as_a_new_column(self):
        self.assertIn(
            IndCqc.proportion_of_time_between_submissions, self.returned_df.columns
        )

    def test_returned_proportion_of_time_between_submissions_values_match_expected(
        self,
    ):
        self.assertEqual(self.returned_data, self.expected_data)
