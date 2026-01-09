import unittest
from unittest.mock import Mock, patch

import projects._03_independent_cqc._04_model.fargate.model_03_predict as job
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys

PATCH_PATH = "projects._03_independent_cqc._04_model.fargate.model_03_predict"


class ModelPredictTests(unittest.TestCase):
    TEST_BUCKET_NAME = "some_bucket"
    TEST_MODEL_NAME = "my_model"
    TEST_MODEL_REGISTRY = {
        "my_model": {
            MRKeys.version: "1.0.0",
            MRKeys.dependent: "dependent_col",
            MRKeys.features: ["feat1", "feat2"],
        }
    }

    mock_feature_data = Mock(name="feature_data")
    mock_X = Mock(name="X_data")
    mock_y = Mock(name="y_data")

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.paths.generate_predictions_path")
    @patch(f"{PATCH_PATH}.mUtils.create_predictions_dataframe")
    @patch(f"{PATCH_PATH}.vUtils.load_model")
    @patch(f"{PATCH_PATH}.vUtils.get_run_number", return_value=3)
    @patch(f"{PATCH_PATH}.paths.generate_model_path")
    @patch(f"{PATCH_PATH}.tUtils.convert_dataframe_to_numpy")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_feature_data)
    @patch(f"{PATCH_PATH}.validate_model_definition")
    @patch(f"{PATCH_PATH}.paths.generate_features_path")
    @patch(f"{PATCH_PATH}.model_registry", TEST_MODEL_REGISTRY)
    def test_main_runs_successfully(
        self,
        generate_features_path_mock: Mock,
        validate_model_definition_mock: Mock,
        scan_parquet_mock: Mock,
        convert_dataframe_to_numpy_mock: Mock,
        generate_model_path_mock: Mock,
        get_run_number_mock: Mock,
        load_model_mock: Mock,
        create_predictions_dataframe_mock: Mock,
        generate_predictions_path_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        convert_dataframe_to_numpy_mock.side_effect = [(self.mock_X, self.mock_y)]

        job.main(self.TEST_BUCKET_NAME, self.TEST_MODEL_NAME)

        generate_features_path_mock.assert_called_once()
        validate_model_definition_mock.assert_called_once()
        scan_parquet_mock.assert_called_once()
        convert_dataframe_to_numpy_mock.assert_called_once()
        generate_model_path_mock.assert_called_once()
        get_run_number_mock.assert_called_once()
        load_model_mock.assert_called_once()
        create_predictions_dataframe_mock.assert_called_once()
        generate_predictions_path_mock.assert_called_once()
        write_to_parquet_mock.assert_called_once()
