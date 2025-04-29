import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._05_model.jobs.train_linear_regression_model as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    TrainLinearRegressionModelData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    TrainLinearRegressionModelSchema as Schemas,
)
from utils import utils

PATCH_PATH: str = (
    "projects._03_independent_cqc._05_model.jobs.train_linear_regression_model"
)


class Main(unittest.TestCase):
    branch_name = "test_branch"
    care_home_model_name = "test_care_home_model"
    non_res_model_name = "test_non_res_model"
    model_version = "1.0.0"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.feature_rows, Schemas.feature_schema
        )

        warnings.simplefilter("ignore", ResourceWarning)

    @patch(f"{PATCH_PATH}.mUtils.create_test_and_train_datasets")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_non_null_value")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.mUtils.generate_model_s3_path")
    @patch(f"{PATCH_PATH}.mUtils.generate_model_features_s3_path")
    def test_main_when_care_home(
        self,
        generate_model_features_s3_path_mock: Mock,
        generate_model_s3_path_mock: Mock,
        read_from_parquet_mock: Mock,
        select_rows_with_non_null_value_mock: Mock,
        create_test_and_train_datasets_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df
        create_test_and_train_datasets_mock.return_value = (self.test_df, self.test_df)

        job.main(
            self.branch_name,
            self.care_home_model_name,
            self.model_version,
        )

        generate_model_features_s3_path_mock.assert_called_once()
        generate_model_s3_path_mock.assert_called_once()
        read_from_parquet_mock.assert_called_once()
        select_rows_with_non_null_value_mock.assert_called_once()
        create_test_and_train_datasets_mock.assert_called_once()

    @patch(f"{PATCH_PATH}.mUtils.create_test_and_train_datasets")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_non_null_value")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.mUtils.generate_model_s3_path")
    @patch(f"{PATCH_PATH}.mUtils.generate_model_features_s3_path")
    def test_main_when_not_care_home(
        self,
        generate_model_features_s3_path_mock: Mock,
        generate_model_s3_path_mock: Mock,
        read_from_parquet_mock: Mock,
        select_rows_with_non_null_value_mock: Mock,
        create_test_and_train_datasets_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df
        create_test_and_train_datasets_mock.return_value = (self.test_df, self.test_df)

        job.main(
            self.branch_name,
            self.non_res_model_name,
            self.model_version,
        )

        generate_model_features_s3_path_mock.assert_called_once()
        generate_model_s3_path_mock.assert_called_once()
        read_from_parquet_mock.assert_called_once()
        select_rows_with_non_null_value_mock.assert_called_once()
        create_test_and_train_datasets_mock.assert_called_once()
