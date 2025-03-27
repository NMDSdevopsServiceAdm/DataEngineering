import unittest
from unittest.mock import patch, Mock
import warnings

from utils import utils
import utils.estimate_filled_posts.models.non_res_with_and_without_dormancy_combined as job
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelNonResWithAndWithoutDormancyCombinedRows as Data
from tests.test_file_schemas import (
    ModelNonResWithAndWithoutDormancyCombinedSchemas as Schemas,
)


class ModelNonResWithAndWithoutDormancyCombinedTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

    def test_combine_non_res_with_and_without_dormancy_models_runs(self):
        test_df = self.spark.createDataFrame(
            Data.estimated_posts_rows, Schemas.estimated_posts_schema
        )

        returned_df = job.combine_non_res_with_and_without_dormancy_models(test_df)

        pass
