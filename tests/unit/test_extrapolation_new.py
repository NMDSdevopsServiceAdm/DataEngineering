import unittest
from unittest.mock import ANY, Mock, patch
import warnings

from pyspark.sql import Window, WindowSpec

import utils.estimate_filled_posts.models.extrapolation as job
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelExtrapolation as Data
from tests.test_file_schemas import ModelExtrapolation as Schemas


class ModelExtrapolationTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.extrapolation_df = self.spark.createDataFrame(
            Data.extrapolation_rows, Schemas.extrapolation_schema
        )
        self.model_column_name = IndCqc.rolling_average_model

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(ModelExtrapolationTests):
    def setUp(self) -> None:
        super().setUp()

        self.returned_df = job.model_extrapolation(
            self.extrapolation_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            self.model_column_name,
        )

    def test_model_extrapolation_row_count_unchanged(self):
        self.assertEqual(self.returned_df.count(), self.extrapolation_df.count())

    def test_model_extrapolation_returns_new_column(self):
        self.assertIn(IndCqc.extrapolation_model, self.returned_df.columns)


class DefineWindowSpecsTests(ModelExtrapolationTests):
    def setUp(self) -> None:
        super().setUp()

    def test_define_window_spec_return_type(self):
        returned_window_specs = job.define_window_specs()
        self.assertIsInstance(returned_window_specs, tuple)
        self.assertEqual(len(returned_window_specs), 2)
        self.assertIsInstance(returned_window_specs[0], WindowSpec)
        self.assertIsInstance(returned_window_specs[1], WindowSpec)


class CalculateFirstAndLastSubmissionDatesTests(ModelExtrapolationTests):
    def setUp(self) -> None:
        super().setUp()

        self.input_df = self.spark.createDataFrame(
            Data.first_and_last_submission_dates_rows,
            Schemas.first_and_final_submission_dates_schema,
        )
        self.column_with_null_values = IndCqc.ascwds_filled_posts_dedup_clean
        self.window_spec_all_rows = (
            Window.partitionBy(IndCqc.location_id)
            .orderBy(IndCqc.unix_time)
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        self.returned_df = job.calculate_first_and_final_submission_dates(
            self.input_df,
            self.column_with_null_values,
            self.window_spec_all_rows,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_first_and_last_submission_dates_rows,
            Schemas.expected_first_and_final_submission_dates_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    @patch("utils.estimate_filled_posts.models.extrapolation_new.get_selected_value")
    def test_calculate_first_and_final_submission_dates_calls_correct_functions(
        self,
        get_selected_value_mock: Mock,
    ):
        get_selected_value_mock.return_value = self.input_df

        job.calculate_first_and_final_submission_dates(
            self.input_df,
            self.column_with_null_values,
            self.window_spec_all_rows,
        )

        self.assertEqual(get_selected_value_mock.call_count, 2)

        get_selected_value_mock.assert_any_call(
            self.input_df,
            self.window_spec_all_rows,
            self.column_with_null_values,
            IndCqc.unix_time,
            IndCqc.first_submission_time,
            "first",
        )
        get_selected_value_mock.assert_any_call(
            self.input_df,
            self.window_spec_all_rows,
            self.column_with_null_values,
            IndCqc.unix_time,
            IndCqc.final_submission_time,
            "last",
        )

    def test_calculate_first_and_final_submission_dates_returns_same_number_of_rows(
        self,
    ):
        self.assertEqual(self.input_df.count(), self.returned_df.count())

    def test_calculate_first_and_final_submission_dates_returns_new_columns(self):
        self.assertIn(IndCqc.first_submission_time, self.returned_df.columns)
        self.assertIn(IndCqc.final_submission_time, self.returned_df.columns)

    def test_returned_values_match_expected(self):
        self.assertEqual(self.returned_data, self.expected_data)


class ExtrapolationForwardsTests(ModelExtrapolationTests):
    def setUp(self) -> None:
        super().setUp()

        self.input_df = self.spark.createDataFrame(
            Data.extrapolation_forwards_rows,
            Schemas.extrapolation_forwards_schema,
        )
        self.column_with_null_values = IndCqc.ascwds_filled_posts_dedup_clean
        self.model_to_extrapolate_from = IndCqc.rolling_average_model
        self.window_spec_lagged = (
            Window.partitionBy(IndCqc.location_id)
            .orderBy(IndCqc.unix_time)
            .rowsBetween(Window.unboundedPreceding, -1)
        )
        self.mock_df = self.spark.createDataFrame(
            Data.extrapolation_forwards_mock_rows,
            Schemas.extrapolation_forwards_mock_schema,
        )
        self.returned_df = job.extrapolation_forwards(
            self.input_df,
            self.column_with_null_values,
            self.model_to_extrapolate_from,
            self.window_spec_lagged,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_extrapolation_forwards_rows,
            Schemas.expected_extrapolation_forwards_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    @patch("utils.estimate_filled_posts.models.extrapolation_new.get_selected_value")
    def test_extrapolation_forwards_calls_correct_functions(
        self,
        get_selected_value_mock: Mock,
    ):
        get_selected_value_mock.return_value = self.mock_df

        job.extrapolation_forwards(
            self.input_df,
            self.column_with_null_values,
            self.model_to_extrapolate_from,
            self.window_spec_lagged,
        )

        self.assertEqual(get_selected_value_mock.call_count, 2)

        get_selected_value_mock.assert_any_call(
            self.input_df,
            self.window_spec_lagged,
            self.column_with_null_values,
            self.column_with_null_values,
            IndCqc.previous_non_null_value,
            "last",
        )
        get_selected_value_mock.assert_any_call(
            ANY,
            self.window_spec_lagged,
            self.column_with_null_values,
            self.model_to_extrapolate_from,
            IndCqc.previous_model_value,
            "last",
        )

    def test_extrapolation_forwards_returns_same_number_of_rows(
        self,
    ):
        self.assertEqual(self.input_df.count(), self.returned_df.count())

    def test_extrapolation_forwards_added_as_a_new_column(self):
        self.assertIn(IndCqc.extrapolation_forwards, self.returned_df.columns)

    def test_returned_extrapolation_forwards_values_match_expected(self):
        self.assertEqual(self.returned_data, self.expected_data)


class ExtrapolationBackwardsTests(ModelExtrapolationTests):
    def setUp(self) -> None:
        super().setUp()

        self.input_df = self.spark.createDataFrame(
            Data.extrapolation_backwards_rows,
            Schemas.extrapolation_backwards_schema,
        )
        self.column_with_null_values = IndCqc.ascwds_filled_posts_dedup_clean
        self.model_to_extrapolate_from = IndCqc.rolling_average_model
        self.window_spec_all_rows = (
            Window.partitionBy(IndCqc.location_id)
            .orderBy(IndCqc.unix_time)
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        self.mock_df = self.spark.createDataFrame(
            Data.extrapolation_backwards_mock_rows,
            Schemas.extrapolation_backwards_mock_schema,
        )
        self.returned_df = job.extrapolation_backwards(
            self.input_df,
            self.column_with_null_values,
            self.model_to_extrapolate_from,
            self.window_spec_all_rows,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_extrapolation_backwards_rows,
            Schemas.expected_extrapolation_backwards_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    @patch("utils.estimate_filled_posts.models.extrapolation_new.get_selected_value")
    def test_extrapolation_backwards_calls_correct_functions(
        self,
        get_selected_value_mock: Mock,
    ):
        get_selected_value_mock.return_value = self.mock_df

        job.extrapolation_backwards(
            self.input_df,
            self.column_with_null_values,
            self.model_to_extrapolate_from,
            self.window_spec_all_rows,
        )

        self.assertEqual(get_selected_value_mock.call_count, 2)

        get_selected_value_mock.assert_any_call(
            self.input_df,
            self.window_spec_all_rows,
            self.column_with_null_values,
            self.column_with_null_values,
            IndCqc.first_non_null_value,
            "first",
        )
        get_selected_value_mock.assert_any_call(
            ANY,
            self.window_spec_all_rows,
            self.column_with_null_values,
            self.model_to_extrapolate_from,
            IndCqc.first_model_value,
            "first",
        )

    def test_extrapolation_backwards_returns_same_number_of_rows(
        self,
    ):
        self.assertEqual(self.input_df.count(), self.returned_df.count())

    def test_extrapolation_backwards_added_as_a_new_column(self):
        self.assertIn(IndCqc.extrapolation_backwards, self.returned_df.columns)

    def test_returned_extrapolation_backwards_values_match_expected(self):
        self.assertEqual(self.returned_data, self.expected_data)


class CombineExtrapolationTests(ModelExtrapolationTests):
    def setUp(self):
        super().setUp()

        test_combine_extrapolation_df = self.spark.createDataFrame(
            Data.combine_extrapolation_rows,
            Schemas.combine_extrapolation_schema,
        )
        self.returned_df = job.combine_extrapolation(test_combine_extrapolation_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_combine_extrapolation_rows,
            Schemas.expected_combine_extrapolation_schema,
        )
        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_combine_extrapolation_returns_expected_columns(self):
        self.assertTrue(self.returned_df.columns, self.expected_df.columns)

    def test_combine_extrapolation_returns_expected_values(self):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][IndCqc.extrapolation_model],
                self.expected_data[i][IndCqc.extrapolation_model],
                f"Returned value in row {i} does not match expected",
            )
