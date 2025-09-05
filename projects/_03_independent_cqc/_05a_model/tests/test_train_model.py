import os
import unittest
from unittest.mock import patch, MagicMock

import projects._03_independent_cqc._05a_model.fargate.train_model as job

PATCH_PATH = "projects._03_independent_cqc._05a_model.fargate.train_model"


class TestMain(unittest.TestCase):
    def setUp(self):
        self.original_s3_bucket = os.getenv("MODEL_S3_BUCKET")
        os.environ["MODEL_S3_BUCKET"] = "test_bucket"
        self.original_s3_prefix = os.getenv("MODEL_S3_PREFIX")
        os.environ["MODEL_S3_PREFIX"] = "test_s3_prefix"

    @patch(f"{PATCH_PATH}.ModelVersionManager", return_value=MagicMock())
    @patch(f"{PATCH_PATH}.Model", return_value=MagicMock())
    @patch.dict(
        f"{PATCH_PATH}.model_definitions", {"some_model": {"some_key": "some_value"}}
    )
    def test_main_calls_expected_functions(self, mock_model, mock_version_manager):
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
        result = job.main(
            model_name="some_model", raw_data_bucket="test_raw_data_bucket"
        )

        # THEN
        mock_model.assert_called_once_with(**{"some_key": "some_value"})
        mock_model.return_value.get_raw_data.assert_called_once_with(
            bucket_name="test_raw_data_bucket"
        )
        mock_model.create_train_and_test_datasets.assert_called_once_with(mock_raw_data)
        mock_model.return_value.fit.assert_called_once_with(mock_train_data)
        mock_model.return_value.validate.assert_called_once_with(mock_test_data)
        mock_version_manager.assert_called_once_with(
            s3_bucket="test_bucket",
            s3_prefix="test_s3_prefix",
            param_store_name="some_param_location",
            default_patch=True,
        )
        mock_version_manager.return_value.prompt_and_save.assert_called_once_with(
            model=mock_fitted_model
        )
        self.assertEqual(
            {
                "train_score": 0.456,
                "test_score": 0.789,
                "score_difference": 0.123,
            },
            result,
        )

    def tearDown(self):
        if self.original_s3_bucket is not None:
            os.environ["MODEL_S3_BUCKET"] = self.original_s3_bucket
        else:
            del os.environ["MODEL_S3_BUCKET"]

        if self.original_s3_prefix is not None:
            os.environ["MODEL_S3_PREFIX"] = self.original_s3_prefix
        else:
            del os.environ["MODEL_S3_PREFIX"]
