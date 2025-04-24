import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import jobs.train_linear_regression_model as job
from tests.test_file_data import TrainLinearRegressionModelData as Data
from tests.test_file_schemas import TrainLinearRegressionModelSchema as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = "jobs.train_linear_regression_model"


class Main(unittest.TestCase):
    features_source = "some/source"
    model_source = "s3://pipeline-resources/models/prediction/1.0.0/"
    metrics_destination = "some/destination"
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
    @patch(f"{PATCH_PATH}.utils.select_rows_with_non_null_value")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_when_care_home(
        self,
        read_from_parquet_mock: Mock,
        select_rows_with_non_null_value_mock: Mock,
        train_lasso_regression_model_mock: Mock,
        save_model_to_s3_mock: Mock,
        save_model_metrics_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.features_source,
            self.model_source,
            self.metrics_destination,
            is_care_home_model=True,
        )

        read_from_parquet_mock.assert_called_once()
        select_rows_with_non_null_value_mock.assert_called_once()
        train_lasso_regression_model_mock.assert_called_once()
        save_model_to_s3_mock.assert_called_once()
        save_model_metrics_mock.assert_called_once_with(
            ANY,
            self.dependent_variable_when_care_home,
            ANY,
            self.metrics_destination,
        )

    @patch(f"{PATCH_PATH}.save_model_metrics")
    @patch(f"{PATCH_PATH}.mUtils.save_model_to_s3")
    @patch(f"{PATCH_PATH}.mUtils.train_lasso_regression_model")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_non_null_value")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_when_not_care_home(
        self,
        read_from_parquet_mock: Mock,
        select_rows_with_non_null_value_mock: Mock,
        train_lasso_regression_model_mock: Mock,
        save_model_to_s3_mock: Mock,
        save_model_metrics_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.features_source,
            self.model_source,
            self.metrics_destination,
        )

        read_from_parquet_mock.assert_called_once()
        select_rows_with_non_null_value_mock.assert_called_once()
        train_lasso_regression_model_mock.assert_called_once()
        save_model_to_s3_mock.assert_called_once()
        save_model_metrics_mock.assert_called_once_with(
            ANY,
            self.dependent_variable_when_not_care_home,
            ANY,
            self.metrics_destination,
        )
