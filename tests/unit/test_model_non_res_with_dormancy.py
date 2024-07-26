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

    @patch(
        "utils.estimate_filled_posts.models.non_res_with_dormancy.save_model_metrics"
    )
    def test_model_non_res_with_dormancy_runs(
        self,
        save_model_metrics: Mock,
    ):
        job.model_non_res_with_dormancy(
            self.non_res_with_dormancy_cleaned_ind_cqc_df,
            self.non_res_with_dormancy_features_df,
            self.NON_RES_WITH_DORMANCY_MODEL,
            self.METRICS_DESTINATION,
        )

        self.assertEqual(save_model_metrics.call_count, 1)

    @patch(
        "utils.estimate_filled_posts.models.non_res_with_dormancy.save_model_metrics"
    )
    def test_model_non_res_with_dormancy_returns_expected_data(
        self,
        save_model_metrics: Mock,
    ):
        df = job.model_non_res_with_dormancy(
            self.non_res_with_dormancy_cleaned_ind_cqc_df,
            self.non_res_with_dormancy_features_df,
            self.NON_RES_WITH_DORMANCY_MODEL,
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

        self.assertIsNotNone(
            expected_location_with_prediction.non_res_with_dormancy_model
        )
        self.assertIsNone(
            expected_location_without_prediction.non_res_with_dormancy_model
        )
