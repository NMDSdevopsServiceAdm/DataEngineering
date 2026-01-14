import unittest
import warnings
from datetime import date
from unittest.mock import Mock, patch

import projects._03_independent_cqc._06_estimate_filled_posts.utils.models.non_res_without_dormancy as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelNonResWithoutDormancy as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelNonResWithoutDormancy as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

PATCH_PATH: str = (
    "projects._03_independent_cqc._06_estimate_filled_posts.utils.models.non_res_without_dormancy"
)


class TestModelNonResWithDormancy(unittest.TestCase):
    NON_RES_WITHOUT_DORMANCY_MODEL = (
        "tests/test_models/non_residential_without_dormancy_prediction/1.0.0/"
    )

    def setUp(self):
        self.spark = utils.get_spark()
        self.non_res_without_dormancy_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.non_res_without_dormancy_cleaned_ind_cqc_rows,
            Schemas.non_res_without_dormancy_cleaned_ind_cqc_schema,
        )
        self.non_res_without_dormancy_features_df = self.spark.createDataFrame(
            Data.non_res_without_dormancy_features_rows,
            Schemas.non_res_without_dormancy_features_schema,
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    @patch(f"{PATCH_PATH}.prepare_predictions_and_join_into_df")
    @patch(f"{PATCH_PATH}.set_min_value")
    def test_model_non_res_without_dormancy_runs(
        self,
        set_min_value_mock: Mock,
        prepare_predictions_and_join_into_df_mock: Mock,
    ):
        job.model_non_res_without_dormancy(
            self.non_res_without_dormancy_cleaned_ind_cqc_df,
            self.non_res_without_dormancy_features_df,
            self.NON_RES_WITHOUT_DORMANCY_MODEL,
        )

        set_min_value_mock.assert_called_once()
        prepare_predictions_and_join_into_df_mock.assert_called_once()

    def test_model_non_res_without_dormancy_returns_expected_data(self):
        df = job.model_non_res_without_dormancy(
            self.non_res_without_dormancy_cleaned_ind_cqc_df,
            self.non_res_without_dormancy_features_df,
            self.NON_RES_WITHOUT_DORMANCY_MODEL,
        )

        self.assertEqual(df.count(), 3)

        expected_location_with_prediction = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 3, 29))
        ).collect()[0]
        expected_location_without_prediction = df.where(
            df[IndCqc.location_id] == "1-000000002"
        ).collect()[0]

        self.assertIsNotNone(
            expected_location_with_prediction[IndCqc.non_res_without_dormancy_model]
        )
        self.assertIsNone(
            expected_location_without_prediction[IndCqc.non_res_without_dormancy_model]
        )
