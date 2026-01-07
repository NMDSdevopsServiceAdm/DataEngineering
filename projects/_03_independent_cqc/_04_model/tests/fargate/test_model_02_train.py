import unittest
from unittest.mock import Mock, patch

import projects._03_independent_cqc._04_model.fargate.model_02_train as job
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys

PATCH_PATH = "projects._03_independent_cqc._04_model.fargate.model_02_train"


class ModelTrainTests(unittest.TestCase):
    TEST_BUCKET_NAME = "some_bucket"
    TEST_MODEL_NAME = "my_model"
    TEST_MODEL_REGISTRY_RETRAIN = {
        "my_model": {
            MRKeys.version: "1.0.0",
            MRKeys.auto_retrain: True,
            MRKeys.model_type: "lasso",
            MRKeys.model_params: {"alpha": 0.1},
            MRKeys.dependent: "dependent_col",
            MRKeys.features: ["feat1", "feat2"],
        }
    }
    TEST_MODEL_REGISTRY_NO_RETRAIN = {
        "my_model": {
            MRKeys.version: "1.0.0",
            MRKeys.auto_retrain: False,
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
    @patch(f"{PATCH_PATH}.paths.generate_model_path")
    @patch(f"{PATCH_PATH}.mUtils.calculate_metrics")
    @patch(f"{PATCH_PATH}.mUtils.build_model")
    @patch(f"{PATCH_PATH}.tUtils.convert_dataframe_to_numpy")
    @patch(f"{PATCH_PATH}.tUtils.split_train_test")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_feature_data)
    @patch(f"{PATCH_PATH}.validate_model_definition")
    @patch(f"{PATCH_PATH}.paths.generate_features_path")
    @patch(f"{PATCH_PATH}.model_registry", TEST_MODEL_REGISTRY_RETRAIN)
    def test_main_runs_successfully_when_model_is_auto_retrained(
        self,
        generate_features_path_mock: Mock,
        validate_model_definition_mock: Mock,
        scan_parquet_mock: Mock,
        split_train_test_mock: Mock,
        convert_dataframe_to_numpy_mock: Mock,
        build_model_mock: Mock,
        calculate_metrics_mock: Mock,
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

        job.main(self.TEST_BUCKET_NAME, self.TEST_MODEL_NAME)

        generate_features_path_mock.assert_called_once()
        validate_model_definition_mock.assert_called_once()
        scan_parquet_mock.assert_called_once()
        split_train_test_mock.assert_called_once()
        self.assertEqual(convert_dataframe_to_numpy_mock.call_count, 2)
        build_model_mock.assert_called_once()
        calculate_metrics_mock.assert_called_once()
        generate_model_path_mock.assert_called_once()
        get_run_number_mock.assert_called_once()
        save_model_and_metadata_mock.assert_called_once()

    @patch(f"{PATCH_PATH}.vUtils.save_model_and_metadata")
    @patch(f"{PATCH_PATH}.vUtils.get_run_number", return_value=3)
    @patch(f"{PATCH_PATH}.paths.generate_model_path")
    @patch(f"{PATCH_PATH}.mUtils.calculate_metrics")
    @patch(f"{PATCH_PATH}.mUtils.build_model")
    @patch(f"{PATCH_PATH}.tUtils.convert_dataframe_to_numpy")
    @patch(f"{PATCH_PATH}.tUtils.split_train_test")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_feature_data)
    @patch(f"{PATCH_PATH}.validate_model_definition")
    @patch(f"{PATCH_PATH}.paths.generate_features_path")
    @patch(f"{PATCH_PATH}.model_registry", TEST_MODEL_REGISTRY_NO_RETRAIN)
    def test_main_skips_when_auto_retrain_false(
        self,
        generate_features_path_mock: Mock,
        validate_model_definition_mock: Mock,
        scan_parquet_mock: Mock,
        split_train_test_mock: Mock,
        convert_dataframe_to_numpy_mock: Mock,
        build_model_mock: Mock,
        calculate_metrics_mock: Mock,
        generate_model_path_mock: Mock,
        get_run_number_mock: Mock,
        save_model_and_metadata_mock: Mock,
    ):

        job.main(self.TEST_BUCKET_NAME, self.TEST_MODEL_NAME)

        generate_features_path_mock.assert_called_once()
        validate_model_definition_mock.assert_called_once()

        scan_parquet_mock.assert_not_called()
        split_train_test_mock.assert_not_called()
        convert_dataframe_to_numpy_mock.assert_not_called()
        build_model_mock.assert_not_called()
        calculate_metrics_mock.assert_not_called()
        generate_model_path_mock.assert_not_called()
        get_run_number_mock.assert_not_called()
        save_model_and_metadata_mock.assert_not_called()
