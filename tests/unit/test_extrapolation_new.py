import unittest
import warnings

from pyspark.sql import Window

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
        self.window_spec = Window.partitionBy(IndCqc.location_id).orderBy(
            IndCqc.unix_time
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_extrapolation_row_count_unchanged(self):
        output_df = job.model_extrapolation(
            self.extrapolation_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            self.model_column_name,
            self.extrapolation_model_column_name,
            self.window_spec,
        )
        self.assertEqual(output_df.count(), self.extrapolation_df.count())
