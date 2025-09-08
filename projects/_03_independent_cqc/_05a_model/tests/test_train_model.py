import os
import unittest
from unittest.mock import patch, MagicMock
from projects._03_independent_cqc._05a_model.utils.model import ModelType

import projects._03_independent_cqc._05a_model.fargate.train_model as job

PATCH_PATH = "projects._03_independent_cqc._05a_model.fargate.train_model"

mock_model_type = MagicMock(spec=ModelType)
mock_model_type.SILLY = "silly"

invalid_definition = {
    "model_type": mock_model_type.SILLY,
    "model_identifier": "non_res_pir",
    "model_params": dict(),
    "version_parameter_location": f"/models/test/non_res_pir",
    "data_source_prefix": "domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/",
    "target_columns": ["ascwds_filled_posts_deduplicated_clean"],
    "feature_columns": ["pir_people_directly_employed_deduplicated"],
}


@patch(f"{PATCH_PATH}.utils.send_sns_notification")
class TestMain(unittest.TestCase):
    @patch(f"{PATCH_PATH}.ModelVersionManager", return_value=MagicMock())
    @patch(f"{PATCH_PATH}.Model", return_value=MagicMock())
    @patch.dict(
        f"{PATCH_PATH}.model_definitions", {"some_model": {"some_key": "some_value"}}
    )
    def test_calls_expected_functions(
        self, mock_model, mock_version_manager, mock_sns_notification
    ):
        # GIVEN
        mock_raw_data = MagicMock()
        mock_model.return_value.get_raw_data.return_value = mock_raw_data
        mock_train_data = MagicMock()
        mock_test_data = MagicMock()
        mock_model.create_train_and_test_datasets.return_value = (
            mock_train_data,
            mock_test_data,
        )
        mock_fitted_model = MagicMock()
        mock_model.return_value.fit.return_value = mock_fitted_model
        mock_model.return_value.validate.return_value = 0.123
        mock_model.return_value.training_score = 0.456
        mock_model.return_value.testing_score = 0.789
        mock_model.return_value.version_parameter_location = "some_param_location"

        # WHEN
        job.main(model_name="some_model", raw_data_bucket="test_raw_data_bucket")

        # THEN
        mock_model.assert_called_once_with(**{"some_key": "some_value"})
        mock_model.return_value.get_raw_data.assert_called_once_with(
            bucket_name="test_raw_data_bucket"
        )
        mock_model.create_train_and_test_datasets.assert_called_once_with(mock_raw_data)
        mock_model.return_value.fit.assert_called_once_with(mock_train_data)
        mock_model.return_value.validate.assert_called_once_with(mock_test_data)
        mock_version_manager.assert_called_once_with(
            s3_bucket="test_model_s3_bucket",
            s3_prefix="test_model_s3_prefix",
            param_store_name="some_param_location",
            default_patch=True,
        )
        mock_version_manager.return_value.prompt_and_save.assert_called_once_with(
            model=mock_model()
        )
        mock_sns_notification.assert_called_once()

    def test_raises_key_error_and_logs_if_unrecognised_model_name(
        self, mock_sns_notification
    ):
        with self.assertLogs(level="ERROR") as cm:
            with self.assertRaises(KeyError):
                job.main(model_name="silly_model", raw_data_bucket="silly_bucket")
        self.assertIn("Check that the model name is valid.", cm.output[2])

    @patch.dict(f"{PATCH_PATH}.model_definitions", {"some_model": invalid_definition})
    def test_raises_value_error_and_logs_if_invalid_model_type(
        self, mock_sns_notification
    ):
        with self.assertLogs(level="ERROR") as cm:
            with self.assertRaises(ValueError):
                job.main(model_name="some_model", raw_data_bucket="silly_bucket")
        self.assertIn(
            "Check that you specified a valid model_type in your model definition.",
            cm.output[2],
        )

    @patch.dict(
        f"{PATCH_PATH}.model_definitions", {"some_model": {"some_key": "some_value"}}
    )
    def test_raises_type_error_and_logs_if_invalid_model_parameters(
        self, mock_sns_notification
    ):
        with self.assertLogs(level="ERROR") as cm:
            with self.assertRaises(TypeError):
                job.main(model_name="some_model", raw_data_bucket="silly_bucket")
        self.assertIn(
            "It is likely the model failed to instantiate. Check the parameters.",
            cm.output[2],
        )
