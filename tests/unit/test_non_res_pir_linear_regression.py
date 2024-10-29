import unittest
from unittest.mock import patch, Mock
import warnings

from utils import utils
import utils.estimate_filled_posts.models.non_res_pir_linear_regression as job
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelNonResPirLinearRegressionRows as Data
from tests.test_file_schemas import ModelNonResPirLinearRegressionSchemas as Schemas


class TestModelNonResPirLinearRegression(unittest.TestCase):
    NON_RES_PIR_MODEL = (
        "tests/test_models/non_res_pir_linear_regression_prediction/1.0.0/"
    )
    METRICS_DESTINATION = "metrics destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.ind_cqc_df = self.spark.createDataFrame(
            Data.non_res_pir_cleaned_ind_cqc_rows,
            Schemas.non_res_pir_cleaned_ind_cqc_schema,
        )
        self.non_res_pir_features_df = self.spark.createDataFrame(
            Data.non_res_pir_features_rows,
            Schemas.non_res_pir_features_schema,
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    @patch(
        "utils.estimate_filled_posts.models.non_res_pir_linear_regression.insert_predictions_into_locations"
    )
    @patch(
        "utils.estimate_filled_posts.models.non_res_pir_linear_regression.save_model_metrics"
    )
    def test_model_non_res_pir_linear_regression_runs(
        self,
        save_model_metrics_mock: Mock,
        insert_predictions_into_locations_mock: Mock,
    ):
        job.model_non_res_pir_linear_regression(
            self.ind_cqc_df,
            self.non_res_pir_features_df,
            self.NON_RES_PIR_MODEL,
            self.METRICS_DESTINATION,
        )

        self.assertEqual(save_model_metrics_mock.call_count, 1)
        self.assertEqual(insert_predictions_into_locations_mock.call_count, 1)

    @patch(
        "utils.estimate_filled_posts.models.non_res_pir_linear_regression.save_model_metrics"
    )
    def test_model_non_res_pir_linear_regression_returns_expected_prediction(
        self,
        save_model_metrics_mock: Mock,
    ):
        test_df = self.spark.createDataFrame(
            Data.non_res_location_with_pir_row,
            Schemas.non_res_pir_cleaned_ind_cqc_schema,
        )
        returned_data = job.model_non_res_pir_linear_regression(
            test_df,
            self.non_res_pir_features_df,
            self.NON_RES_PIR_MODEL,
            self.METRICS_DESTINATION,
        ).collect()
        expected_data = self.spark.createDataFrame(
            Data.expected_non_res_location_with_pir_row,
            Schemas.expected_non_res_pir_prediction_schema,
        ).collect()

        self.assertAlmostEqual(
            returned_data[0][IndCqc.non_res_pir_linear_regression_model],
            expected_data[0][IndCqc.non_res_pir_linear_regression_model],
            places=5,
        )
