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
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils import utils

partition_keys = [
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]

PATCH_PATH: str = (
    "projects._03_independent_cqc._05_model.jobs.run_linear_regression_model"
)


class Main(unittest.TestCase):
    branch_name = "test_branch"
    care_home_model_name = "test_care_home_model"
    non_res_model_name = "test_non_res_model"
    model_version = "1.0.0"
    model_run_number = 3
    prediction_destination = "some/prediction/destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.feature_rows, Schemas.feature_schema
        )

        warnings.simplefilter("ignore", ResourceWarning)

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.mUtils.set_min_value")
    @patch(f"{PATCH_PATH}.calculate_filled_posts_from_beds_and_ratio")
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
        calculate_filled_posts_from_beds_and_ratio_mock: Mock,
        set_min_value_mock: Mock,
        write_to_parquet_mock: Mock,
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
        calculate_filled_posts_from_beds_and_ratio_mock.assert_called_once()
        set_min_value_mock.assert_called_once()
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            generate_model_predictions_s3_path_mock.return_value,
            "overwrite",
            partition_keys,
        )

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.mUtils.set_min_value")
    @patch(f"{PATCH_PATH}.calculate_filled_posts_from_beds_and_ratio")
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
        calculate_filled_posts_from_beds_and_ratio_mock: Mock,
        set_min_value_mock: Mock,
        write_to_parquet_mock: Mock,
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
        self.assertEqual(calculate_filled_posts_from_beds_and_ratio_mock.call_count, 0)
        set_min_value_mock.assert_called_once()
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            generate_model_predictions_s3_path_mock.return_value,
            "overwrite",
            partition_keys,
        )
