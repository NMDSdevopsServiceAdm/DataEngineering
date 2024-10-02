import unittest
from unittest.mock import Mock, patch
import warnings

import utils.estimate_filled_posts.models.extrapolation_and_interpolation as job
from utils import utils
from tests.test_file_data import ModelExtrapolationAndInterpolation as Data
from tests.test_file_schemas import ModelExtrapolationAndInterpolation as Schemas


class ModelExtrapolationAndInterpolationTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(ModelExtrapolationAndInterpolationTests):
    def setUp(self) -> None:
        super().setUp()

        self.extrapolation_and_interpolation_df = self.spark.createDataFrame(
            Data.extrapolation_and_interpolation_rows,
            Schemas.extrapolation_and_interpolation_schema,
        )
        self.column_with_null_values: str = "null_values_column"
        self.model_column_name: str = "trend_model"

        self.returned_df = job.model_extrapolation_and_interpolation(
            self.extrapolation_and_interpolation_df,
            self.column_with_null_values,
            self.model_column_name,
        )

    @patch(
        "utils.estimate_filled_posts.models.extrapolation_and_interpolation.model_interpolation"
    )
    @patch(
        "utils.estimate_filled_posts.models.extrapolation_and_interpolation.model_extrapolation"
    )
    def test_model_extrapolation_and_interpolation_runs(
        self,
        model_extrapolation_mock: Mock,
        model_interpolation_mock: Mock,
    ):
        job.model_extrapolation_and_interpolation(
            self.extrapolation_and_interpolation_df,
            self.column_with_null_values,
            self.model_column_name,
        )

        model_extrapolation_mock.assert_called_once()
        model_interpolation_mock.assert_called_once()

    def test_model_extrapolation_and_interpolation_returns_same_number_of_rows(self):
        self.assertEqual(
            self.extrapolation_and_interpolation_df.count(), self.returned_df.count()
        )

    def test_model_extrapolation_and_interpolation_returns_new_columns(self):
        extrapolation_model_column_name = "extrapolation_null_values_column_trend_model"
        interpolation_model_column_name = "interpolation_null_values_column_trend_model"

        self.assertIn(extrapolation_model_column_name, self.returned_df.columns)
        self.assertIn(interpolation_model_column_name, self.returned_df.columns)


class CreateNewColumnNamesTests(ModelExtrapolationAndInterpolationTests):
    def setUp(self) -> None:
        super().setUp()

    def test_create_new_column_names_returns_expected_strings(self):
        null_column_name: str = "filled_posts"
        model_column_name: str = "trend_model"
        expected_column_names = (
            "extrapolation_filled_posts_trend_model",
            "interpolation_filled_posts_trend_model",
        )

        self.assertEqual(
            job.create_new_column_names(null_column_name, model_column_name),
            expected_column_names,
        )
