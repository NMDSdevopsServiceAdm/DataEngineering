import unittest
import warnings
from unittest.mock import Mock, patch

import utils.estimate_filled_posts.models.extrapolation_and_interpolation as job
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
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
        self.model_column_name = IndCqc.rolling_average_model

    @patch(
        "utils.estimate_filled_posts.models.extrapolation_and_interpolation.model_interpolation"
    )
    @patch(
        "utils.estimate_filled_posts.models.extrapolation_and_interpolation.model_extrapolation"
    )
    def test_main_runs(
        self,
        model_extrapolation_mock: Mock,
        model_interpolation_mock: Mock,
    ):
        job.model_extrapolation_and_interpolation(
            self.extrapolation_and_interpolation_df,
            self.model_column_name,
            IndCqc.ascwds_filled_posts_dedup_clean,
            IndCqc.interpolation_model_ascwds_filled_posts_dedup_clean,
        )

        model_extrapolation_mock.assert_called_once()
        model_interpolation_mock.assert_called_once()
