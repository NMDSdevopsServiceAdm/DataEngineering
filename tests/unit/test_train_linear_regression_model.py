import unittest
import warnings
from unittest.mock import Mock, patch

import jobs.train_linear_regression_model as job
from tests.test_file_data import TrainLinearRegressionModelData as Data
from tests.test_file_schemas import TrainLinearRegressionModelSchema as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = "jobs.train_linear_regression_model"


class Main(unittest.TestCase):
    features_source = "some/source"
    model_source = "s3://pipeline-resources/models/prediction/1.0.0/"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.feature_rows, Schemas.feature_schema
        )

        warnings.simplefilter("ignore", ResourceWarning)

    @patch(f"{PATCH_PATH}.mUtils.save_model_to_s3")
    @patch(f"{PATCH_PATH}.mUtils.train_lasso_regression_model")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        train_lasso_regression_model_mock: Mock,
        save_model_to_s3_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.features_source,
            self.model_source,
            IndCQC.imputed_filled_post_model,
        )

        read_from_parquet_mock.assert_called_once()
        train_lasso_regression_model_mock.assert_called_once()
        save_model_to_s3_mock.assert_called_once()
