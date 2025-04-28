import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._05_model.jobs.run_linear_regression_model as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    RunLinearRegressionModelData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    RunLinearRegressionModelSchema as Schemas,
)
from utils import utils

PATCH_PATH: str = (
    "projects._03_independent_cqc._05_model.jobs.run_linear_regression_model"
)


class Main(unittest.TestCase):
    branch_name = "test_branch"
    care_home_model_name = "test_care_home_model"
    non_res_model_name = "test_non_res_model"
    model_version = "1.0.0"
    prediction_destination = "some/prediction/destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.feature_rows, Schemas.feature_schema
        )

        warnings.simplefilter("ignore", ResourceWarning)

    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.mUtils.load_latest_model_from_s3")
    @patch(f"{PATCH_PATH}.mUtils.generate_model_s3_path")
    @patch(f"{PATCH_PATH}.mUtils.generate_model_predictions_s3_path")
    @patch(f"{PATCH_PATH}.mUtils.generate_model_features_s3_path")
    def test_main_when_care_home(
        self,
        generate_model_features_s3_path_mock: Mock,
        generate_model_predictions_s3_path_mock: Mock,
        generate_model_s3_path_mock: Mock,
        load_latest_model_from_s3_mock: Mock,
        read_from_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df
        generate_model_predictions_s3_path_mock.return_value = (
            self.prediction_destination
        )

        job.main(
            self.branch_name,
            self.care_home_model_name,
            self.model_version,
        )

        generate_model_features_s3_path_mock.assert_called_once()
        generate_model_predictions_s3_path_mock.assert_called_once()
        generate_model_s3_path_mock.assert_called_once()
        load_latest_model_from_s3_mock.assert_called_once()
        read_from_parquet_mock.assert_called_once()

    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.mUtils.load_latest_model_from_s3")
    @patch(f"{PATCH_PATH}.mUtils.generate_model_s3_path")
    @patch(f"{PATCH_PATH}.mUtils.generate_model_predictions_s3_path")
    @patch(f"{PATCH_PATH}.mUtils.generate_model_features_s3_path")
    def test_main_when_non_res(
        self,
        generate_model_features_s3_path_mock: Mock,
        generate_model_predictions_s3_path_mock: Mock,
        generate_model_s3_path_mock: Mock,
        load_latest_model_from_s3_mock: Mock,
        read_from_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df
        generate_model_predictions_s3_path_mock.return_value = (
            self.prediction_destination
        )

        job.main(
            self.branch_name,
            self.non_res_model_name,
            self.model_version,
        )

        generate_model_features_s3_path_mock.assert_called_once()
        generate_model_predictions_s3_path_mock.assert_called_once()
        generate_model_s3_path_mock.assert_called_once()
        load_latest_model_from_s3_mock.assert_called_once()
        read_from_parquet_mock.assert_called_once()
