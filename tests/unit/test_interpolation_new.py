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
