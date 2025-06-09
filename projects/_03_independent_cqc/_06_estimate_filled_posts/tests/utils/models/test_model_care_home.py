import unittest
from unittest.mock import patch, Mock
import warnings
from datetime import date

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

import projects._03_independent_cqc._06_estimate_filled_posts.utils.models.care_homes as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelCareHomes as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelCareHomes as Schemas,
)


PATCH_PATH: str = (
    "projects._03_independent_cqc._06_estimate_filled_posts.utils.models.care_homes"
)


class TestModelCareHome(unittest.TestCase):
    CARE_HOME_MODEL = "tests/test_models/care_home_filled_posts_prediction/1.0.0/"
    METRICS_DESTINATION = "metrics destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.care_homes_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.care_homes_cleaned_ind_cqc_rows,
            Schemas.care_homes_cleaned_ind_cqc_schema,
        )
        self.care_homes_features_df = self.spark.createDataFrame(
            Data.care_homes_features_rows, Schemas.care_homes_features_schema
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    @patch(f"{PATCH_PATH}.insert_predictions_into_pipeline")
    @patch(f"{PATCH_PATH}.save_model_metrics")
    @patch(f"{PATCH_PATH}.set_min_value")
    @patch(f"{PATCH_PATH}.calculate_filled_posts_from_beds_and_ratio")
    def test_model_care_homes_runs(
        self,
        calculate_filled_posts_from_beds_and_ratio_mock: Mock,
        set_min_value_mock: Mock,
        save_model_metrics_mock: Mock,
        insert_predictions_into_pipeline_mock: Mock,
    ):
        job.model_care_homes(
            self.care_homes_cleaned_ind_cqc_df,
            self.care_homes_features_df,
            self.CARE_HOME_MODEL,
            self.METRICS_DESTINATION,
        )

        calculate_filled_posts_from_beds_and_ratio_mock.assert_called_once()
        set_min_value_mock.assert_called_once()
        save_model_metrics_mock.assert_called_once()
        insert_predictions_into_pipeline_mock.assert_called_once()

    @patch(f"{PATCH_PATH}.save_model_metrics")
    def test_model_care_homes_returns_expected_data(
        self,
        save_model_metrics_mock: Mock,
    ):
        df = job.model_care_homes(
            self.care_homes_cleaned_ind_cqc_df,
            self.care_homes_features_df,
            self.CARE_HOME_MODEL,
            self.METRICS_DESTINATION,
        )

        self.assertEqual(df.count(), 5)

        expected_location_with_prediction = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 3, 29))
        ).collect()[0]
        expected_location_without_prediction = df.where(
            df[IndCqc.location_id] == "1-000000002"
        ).collect()[0]

        self.assertIsNotNone(expected_location_with_prediction[IndCqc.care_home_model])
        self.assertIsNone(expected_location_without_prediction[IndCqc.care_home_model])
