import unittest
from unittest.mock import patch, Mock

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

        self.test_df = self.spark.createDataFrame(
            Data.estimated_posts_rows, Schemas.estimated_posts_schema
        )

    # TODO - this can be removed once process complete,
    def test_data_looks_as_expected(self):
        returned_df = job.combine_non_res_with_and_without_dormancy_models(self.test_df)
        # returned_df.sort(IndCqc.location_id).show()
        pass

    @patch("utils.utils.select_rows_with_value")
    def test_models_runs(self, select_rows_with_value_mock: Mock):
        returned_df = job.combine_non_res_with_and_without_dormancy_models(self.test_df)

        select_rows_with_value_mock.assert_called_once()
