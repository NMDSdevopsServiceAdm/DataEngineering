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


class TestModelExtrapolation(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.extrapolation_df = self.spark.createDataFrame(
            Data.extrapolation_new_rows, Schemas.extrapolation_schema
        )
        self.extrapolation_model_column_name = "extrapolation_rolling_average_model"
        self.model_column_name = IndCqc.rolling_average_model

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(TestModelExtrapolation):
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


class DefineWindowSpecsTests(TestModelExtrapolation):
    def setUp(self) -> None:
        super().setUp()

    def test_define_window_spec_return_type(self):
        returned_window_specs = job.define_window_specs()
        self.assertIsInstance(returned_window_specs, tuple)
        self.assertEqual(len(returned_window_specs), 2)
        self.assertIsInstance(returned_window_specs[0], WindowSpec)
        self.assertIsInstance(returned_window_specs[1], WindowSpec)


class CalculateFirstAndLastSubmissionDatesTests(TestModelExtrapolation):
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
