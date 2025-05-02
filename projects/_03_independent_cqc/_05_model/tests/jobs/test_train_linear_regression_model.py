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
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._05_model.jobs.train_linear_regression_model"
)


class Main(unittest.TestCase):
    s3_datasets_uri = "s3://sfc-branch-name-datasets"
    care_home_model_name = "test_care_home_model"
    non_res_model_name = "test_non_res_model"
    model_version = "1.0.0"
    model_run_number = 3
    dependent_variable_when_care_home = IndCQC.imputed_filled_posts_per_bed_ratio_model
    dependent_variable_when_not_care_home = IndCQC.imputed_filled_post_model

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.feature_rows, Schemas.feature_schema
        )

        warnings.simplefilter("ignore", ResourceWarning)

    @patch(f"{PATCH_PATH}.save_model_metrics")
    @patch(f"{PATCH_PATH}.mUtils.save_model_to_s3")
    @patch(f"{PATCH_PATH}.mUtils.train_lasso_regression_model")
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
        train_lasso_regression_model_mock: Mock,
        save_model_to_s3_mock: Mock,
        save_model_metrics_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df
        create_test_and_train_datasets_mock.return_value = (self.test_df, self.test_df)
        save_model_to_s3_mock.return_value = self.model_run_number

        job.main(
            self.s3_datasets_uri,
            self.care_home_model_name,
            self.model_version,
        )

        generate_model_features_s3_path_mock.assert_called_once()
        generate_model_s3_path_mock.assert_called_once()
        read_from_parquet_mock.assert_called_once()
        select_rows_with_non_null_value_mock.assert_called_once()
        train_lasso_regression_model_mock.assert_called_once()
        save_model_to_s3_mock.assert_called_once()
        save_model_metrics_mock.assert_called_once_with(
            ANY,
            ANY,
            self.dependent_variable_when_care_home,
            self.s3_datasets_uri,
            self.care_home_model_name,
            self.model_version,
            save_model_to_s3_mock.return_value,
        )

    @patch(f"{PATCH_PATH}.save_model_metrics")
    @patch(f"{PATCH_PATH}.mUtils.save_model_to_s3")
    @patch(f"{PATCH_PATH}.mUtils.train_lasso_regression_model")
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
        train_lasso_regression_model_mock: Mock,
        save_model_to_s3_mock: Mock,
        save_model_metrics_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df
        create_test_and_train_datasets_mock.return_value = (self.test_df, self.test_df)
        save_model_to_s3_mock.return_value = self.model_run_number

        job.train_linear_regression_model(
            self.s3_datasets_uri,
            self.non_res_model_name,
            self.model_version,
        )

        generate_model_features_s3_path_mock.assert_called_once()
        generate_model_s3_path_mock.assert_called_once()
        read_from_parquet_mock.assert_called_once()
        select_rows_with_non_null_value_mock.assert_called_once()
        train_lasso_regression_model_mock.assert_called_once()
        save_model_to_s3_mock.assert_called_once()
        save_model_metrics_mock.assert_called_once_with(
            ANY,
            ANY,
            self.dependent_variable_when_not_care_home,
            self.s3_datasets_uri,
            self.non_res_model_name,
            self.model_version,
            save_model_to_s3_mock.return_value,
        )
