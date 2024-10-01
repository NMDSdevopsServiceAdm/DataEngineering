import unittest
from unittest.mock import ANY, Mock, patch
import warnings

from pyspark.sql import Window, WindowSpec

import utils.estimate_filled_posts.models.interpolation_new as job
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelInterpolationNew as Data
from tests.test_file_schemas import ModelInterpolationNew as Schemas


class ModelInterpolationTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.interpolation_df = self.spark.createDataFrame(
            Data.interpolation_rows, Schemas.interpolation_schema
        )
        self.interpolation_model_column_name = "interpolation_rolling_average_model"

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(ModelInterpolationTests):
    def setUp(self) -> None:
        super().setUp()

        self.returned_df = job.model_interpolation(
            self.interpolation_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            self.interpolation_model_column_name,
        )

    def test_model_interpolation_row_count_unchanged(self):
        self.assertEqual(self.returned_df.count(), self.interpolation_df.count())

    def test_model_interpolation_and_interpolation_returns_new_columns(self):
        self.assertIn(self.interpolation_model_column_name, self.returned_df.columns)


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

    def test_combine_interpolation_returns_none_when_extrapolation_forwards_is_none(
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

    def test_combine_interpolation_returns_expected_values_when_extrapolation_forwards_is_known(
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

    def test_combine_interpolation_returns_none_date_after_final_non_null_submission(
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
