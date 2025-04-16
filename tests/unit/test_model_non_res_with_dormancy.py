import unittest
from unittest.mock import patch, Mock
import warnings
from datetime import date

from utils import utils
import utils.estimate_filled_posts.models.non_res_with_dormancy as job
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelNonResWithDormancy as Data
from tests.test_file_schemas import ModelNonResWithDormancy as Schemas


class TestModelNonResWithDormancy(unittest.TestCase):
    NON_RES_WITH_DORMANCY_MODEL = (
        "tests/test_models/non_residential_with_dormancy_prediction/1.0.0/"
    )
    METRICS_DESTINATION = "metrics destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.non_res_with_dormancy_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.non_res_with_dormancy_cleaned_ind_cqc_rows,
            Schemas.non_res_with_dormancy_cleaned_ind_cqc_schema,
        )
        self.non_res_with_dormancy_features_df = self.spark.createDataFrame(
            Data.non_res_with_dormancy_features_rows,
            Schemas.non_res_with_dormancy_features_schema,
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
