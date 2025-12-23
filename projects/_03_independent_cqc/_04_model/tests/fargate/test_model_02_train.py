import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._04_model.fargate.model_02_train as job
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys

PATCH_PATH = "projects._03_independent_cqc._04_model.fargate.model_02_train"


class ModelTrainTests(unittest.TestCase):
    TEST_DATASETS_BUCKET_NAME = "some_data_bucket"
    TEST_RESOURCES_BUCKET_NAME = "some_resources_bucket"
    TEST_BUCKET_NAME = "some_bucket"
    TEST_MODEL_NAME = "some_model"
    TEST_MODEL_REGISTRY = {
        "my_model": {
            MRKeys.version: "1.0.0",
            MRKeys.auto_retrain: True,
            MRKeys.model_type: "lasso",
            MRKeys.model_params: {"alpha": 0.1},
            MRKeys.dependent: "dependent_col",
            MRKeys.features: ["feat1", "feat2"],
        }
    }

    mock_feature_data = Mock(name="feature_data")
    mock_train_data = Mock(name="train_data")
    mock_test_data = Mock(name="test_data")
    mock_X = Mock(name="X_data")
    mock_y = Mock(name="y_data")

    @patch(f"{PATCH_PATH}.vUtils.save_model_and_metadata")
    @patch(f"{PATCH_PATH}.vUtils.get_run_number", return_value=3)
    @patch(f"{PATCH_PATH}.pUtils.generate_model_path")
    @patch(f"{PATCH_PATH}.root_mean_squared_error", return_value=2.5)
    @patch(f"{PATCH_PATH}.r2_score", return_value=0.85)
    @patch(f"{PATCH_PATH}.mUtils.build_model")
    @patch(f"{PATCH_PATH}.tUtils.convert_dataframe_to_numpy")
    @patch(f"{PATCH_PATH}.tUtils.split_train_test")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_feature_data)
    @patch(f"{PATCH_PATH}.dUtils.validate_model_definition")
    @patch(f"{PATCH_PATH}.pUtils.generate_features_path")
    @patch(f"{PATCH_PATH}.model_registry", return_value=TEST_MODEL_REGISTRY)
    def test_main_runs_successfully(
        self,
        model_registry_mock: Mock,
        generate_features_path_mock: Mock,
        validate_model_definition_mock: Mock,
        scan_parquet_mock: Mock,
        split_train_test_mock: Mock,
        convert_dataframe_to_numpy_mock: Mock,
        build_model_mock: Mock,
        r2_mock: Mock,
        rmse_mock: Mock,
        generate_model_path_mock: Mock,
        get_run_number_mock: Mock,
        save_model_and_metadata_mock: Mock,
    ):
        split_train_test_mock.side_effect = [
            (self.mock_train_data, self.mock_test_data)
        ]
        convert_dataframe_to_numpy_mock.side_effect = [
            (self.mock_X, self.mock_y),
            (self.mock_X, self.mock_y),
        ]

        job.main(
            self.TEST_DATASETS_BUCKET_NAME,
            self.TEST_RESOURCES_BUCKET_NAME,
            self.TEST_MODEL_NAME,
        )

        generate_features_path_mock.assert_called_once()
        validate_model_definition_mock.assert_called_once()
        scan_parquet_mock.assert_called_once()
        split_train_test_mock.assert_called_once()
        self.assertEqual(convert_dataframe_to_numpy_mock.call_count, 2)
        build_model_mock.assert_called_once()
        r2_mock.assert_called_once()
        rmse_mock.assert_called_once()
        generate_model_path_mock.assert_called_once()
        get_run_number_mock.assert_called_once()
        save_model_and_metadata_mock.assert_called_once()
