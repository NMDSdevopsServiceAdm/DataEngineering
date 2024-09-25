import unittest
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

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(TestModelExtrapolation):
    def setUp(self) -> None:
        super().setUp()

        self.extrapolation_df = self.spark.createDataFrame(
            Data.extrapolation_new_rows, Schemas.extrapolation_schema
        )
        self.extrapolation_model_column_name = "extrapolation_rolling_average_model"
        self.model_column_name = IndCqc.rolling_average_model
        self.window_spec = Window.partitionBy(IndCqc.location_id).orderBy(
            IndCqc.unix_time
        )
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
