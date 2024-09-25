import unittest
from unittest.mock import Mock, patch
import warnings

from pyspark.sql import Window, WindowSpec

import utils.estimate_filled_posts.models.extrapolation_new as job
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelExtrapolationNew as Data
from tests.test_file_schemas import ModelExtrapolationNew as Schemas


class ModelExtrapolationTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.extrapolation_df = self.spark.createDataFrame(
            Data.extrapolation_new_rows, Schemas.extrapolation_schema
        )
        self.extrapolation_model_column_name = "extrapolation_rolling_average_model"
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
            self.extrapolation_model_column_name,
        )

    def test_model_extrapolation_row_count_unchanged(self):
        self.assertEqual(self.returned_df.count(), self.extrapolation_df.count())

    def test_model_extrapolation_and_interpolation_returns_new_columns(self):
        self.assertIn(self.extrapolation_model_column_name, self.returned_df.columns)


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
            Schemas.first_and_last_submission_dates_schema,
        )
        self.column_with_null_values = IndCqc.ascwds_filled_posts_dedup_clean
        self.window_spec_all_rows = (
            Window.partitionBy(IndCqc.location_id)
            .orderBy(IndCqc.unix_time)
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        self.returned_df = job.calculate_first_and_last_submission_dates(
            self.input_df,
            self.column_with_null_values,
            self.window_spec_all_rows,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_first_and_last_submission_dates_rows,
            Schemas.expected_first_and_last_submission_dates_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    @patch("utils.estimate_filled_posts.models.extrapolation_new.get_selected_value")
    def test_calculate_first_and_last_submission_dates_calls_correct_functions(
        self,
        get_selected_value_mock: Mock,
    ):
        get_selected_value_mock.return_value = self.input_df

        job.calculate_first_and_last_submission_dates(
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
            "min",
        )
        get_selected_value_mock.assert_any_call(
            self.input_df,
            self.window_spec_all_rows,
            self.column_with_null_values,
            IndCqc.unix_time,
            IndCqc.last_submission_time,
            "max",
        )

    def test_calculate_first_and_last_submission_dates_returns_same_number_of_rows(
        self,
    ):
        self.assertEqual(self.input_df.count(), self.returned_df.count())

    def test_model_extrapolation_and_interpolation_returns_new_columns(self):
        self.assertIn(IndCqc.first_submission_time, self.returned_df.columns)
        self.assertIn(IndCqc.last_submission_time, self.returned_df.columns)

    def test_returned_values_match_expected(self):
        self.assertEqual(self.returned_data, self.expected_data)


# TODO - test extrapolation_forwards
# TODO - test extrapolation_backwards


class CombineExtrapolationTests(ModelExtrapolationTests):
    def setUp(self):
        super().setUp()

        self.extrapolation_model_name = "extrapolation_model_name"
        test_combine_extrapolation_df = self.spark.createDataFrame(
            Data.combine_extrapolation_rows,
            Schemas.combine_extrapolation_schema,
        )
        self.returned_df = job.combine_extrapolation(
            test_combine_extrapolation_df, self.extrapolation_model_name
        )
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
                self.returned_data[i][self.extrapolation_model_name],
                self.expected_data[i][self.extrapolation_model_name],
                f"Returned value in row {i} does not match expected",
            )


class GetSelectedValueFunctionTests(ModelExtrapolationTests):
    def setUp(self):
        super().setUp()
        self.w = (
            Window.partitionBy(IndCqc.location_id)
            .orderBy(IndCqc.unix_time)
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

    def test_get_selected_value_returns_correct_values_when_selection_equals_min(self):
        test_df = self.spark.createDataFrame(
            Data.test_min_selection_rows, Schemas.get_selected_value_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_test_min_selection_rows,
            Schemas.expected_get_selected_value_schema,
        )
        returned_df = job.get_selected_value(
            test_df,
            self.w,
            IndCqc.ascwds_filled_posts_dedup_clean,
            IndCqc.rolling_average_model,
            "new_column",
            selection="min",
        )
        self.assertEqual(
            returned_df.sort(IndCqc.location_id, IndCqc.unix_time).collect(),
            expected_df.collect(),
        )

    def test_get_selected_value_returns_correct_values_when_selection_equals_max(self):
        test_df = self.spark.createDataFrame(
            Data.test_max_selection_rows, Schemas.get_selected_value_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_test_max_selection_rows,
            Schemas.expected_get_selected_value_schema,
        )
        returned_df = job.get_selected_value(
            test_df,
            self.w,
            IndCqc.ascwds_filled_posts_dedup_clean,
            IndCqc.rolling_average_model,
            "new_column",
            selection="max",
        )
        self.assertEqual(
            returned_df.sort(IndCqc.location_id, IndCqc.unix_time).collect(),
            expected_df.collect(),
        )

    def test_get_selected_value_returns_correct_values_when_selection_equals_first(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_first_selection_rows, Schemas.get_selected_value_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_test_first_selection_rows,
            Schemas.expected_get_selected_value_schema,
        )
        returned_df = job.get_selected_value(
            test_df,
            self.w,
            IndCqc.ascwds_filled_posts_dedup_clean,
            IndCqc.rolling_average_model,
            "new_column",
            selection="first",
        )
        self.assertEqual(
            returned_df.sort(IndCqc.location_id, IndCqc.unix_time).collect(),
            expected_df.collect(),
        )

    def test_get_selected_value_returns_correct_values_when_selection_equals_last(self):
        test_df = self.spark.createDataFrame(
            Data.test_last_selection_rows, Schemas.get_selected_value_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_test_last_selection_rows,
            Schemas.expected_get_selected_value_schema,
        )
        returned_df = job.get_selected_value(
            test_df,
            self.w,
            IndCqc.ascwds_filled_posts_dedup_clean,
            IndCqc.rolling_average_model,
            "new_column",
            selection="last",
        )
        self.assertEqual(
            returned_df.sort(IndCqc.location_id, IndCqc.unix_time).collect(),
            expected_df.collect(),
        )

    def test_get_selected_value_raises_error_when_selection_is_not_permitted(self):
        test_df = self.spark.createDataFrame(
            Data.test_last_selection_rows, Schemas.get_selected_value_schema
        )

        with self.assertRaises(ValueError) as context:
            job.get_selected_value(
                test_df,
                self.w,
                IndCqc.ascwds_filled_posts_dedup_clean,
                IndCqc.rolling_average_model,
                "new_column",
                selection="other",
            )

        self.assertTrue(
            "Error: The selection parameter 'other' was not found. Please use 'min', 'max', 'first', or 'last'.",
            "Exception does not contain the correct error message",
        )
