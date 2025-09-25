import pickle
import unittest
from unittest.mock import patch, MagicMock
from projects._03_independent_cqc._05_model.utils.model import ModelType, Model
from typing import Any
from tempfile import NamedTemporaryFile
from sklearn.base import BaseEstimator
from sklearn.metrics import r2_score
import polars as pl
from pathlib import Path

from projects._03_independent_cqc._05_model.fargate.retraining.train_model import (
    ModelVersionManager,
    main,
    model_definitions,
)

PATCH_PATH = "projects._03_independent_cqc._05_model.fargate.retraining.train_model"

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

ice_cream_model = {
    "dummy_model": {
        "model_type": ModelType.SIMPLE_LINEAR.value,
        "model_identifier": "dummy_model",
        "model_params": dict(),
        "version_parameter_location": "/param_loc",
        "data_source_prefix": "dummy_prefix",
        "target_columns": ["IceCreamSales"],
        "feature_columns": ["MeanDailyTemperature"],
    }
}

ice_cream_data = pl.read_parquet(
    "projects/_03_independent_cqc/_05_model/tests/short_sales.parquet"
)


def save_to_temp(model: BaseEstimator, version: str) -> str:
    with NamedTemporaryFile(delete=False) as fp:
        pickle.dump(model, fp)
        fp.close()
    return str(Path(fp.name))


def null_action(var: Any) -> None:
    pass


@patch(f"{PATCH_PATH}.utils.send_sns_notification")
class TestMain(unittest.TestCase):
    @patch(f"{PATCH_PATH}.ModelVersionManager", return_value=MagicMock())
    @patch(f"{PATCH_PATH}.Model", return_value=MagicMock())
    @patch.dict(model_definitions, {"some_model": {"some_key": "some_value"}})
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
        main(model_name="some_model")

        # THEN
        mock_model.assert_called_once_with(**{"some_key": "some_value"})
        mock_model.return_value.get_raw_data.assert_called_once_with(
            bucket_name="test_raw_data_bucket"
        )
        mock_model.create_train_and_test_datasets.assert_called_once_with(
            mock_raw_data, seed=None
        )
        mock_model.return_value.fit.assert_called_once_with(mock_train_data)
        mock_model.return_value.validate.assert_called_once_with(mock_test_data)
        mock_version_manager.assert_called_once_with(
            s3_bucket="test_model_s3_bucket",
            s3_prefix="test_model_s3_prefix/some_model",
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
                main(model_name="silly_model")
        self.assertIn("Check that the model name is valid.", cm.output[2])

    @patch.dict(model_definitions, {"some_model": invalid_definition})
    def test_raises_value_error_and_logs_if_invalid_model_type(
        self, mock_sns_notification
    ):
        with self.assertLogs(level="ERROR") as cm:
            with self.assertRaises(ValueError):
                main(model_name="some_model")
        self.assertIn(
            "Check that you specified a valid model_type in your model definition.",
            cm.output[2],
        )

    @patch.dict(
        model_definitions,
        {"some_model": {"some_key": "some_value"}},
    )
    def test_raises_type_error_and_logs_if_invalid_model_parameters(
        self, mock_sns_notification
    ):
        with self.assertLogs(level="ERROR") as cm:
            with self.assertRaises(TypeError):
                main(model_name="some_model")
        self.assertIn(
            "It is likely the model failed to instantiate. Check the parameters.",
            cm.output[2],
        )

    @patch.object(ModelVersionManager, "update_parameter_store")
    @patch.object(ModelVersionManager, "save_model")
    @patch.object(Model, "get_raw_data")
    @patch.dict(
        model_definitions,
        ice_cream_model,
    )
    def test_train_model_saves_expected_model_parameters(
        self, mock_get_data, mock_save, mock_update_parameter_store, mock_sns
    ):
        mock_get_data.return_value = ice_cream_data.lazy()
        mock_save.side_effect = save_to_temp
        mock_update_parameter_store.side_effect = null_action
        vm = main(model_name="dummy_model", seed=1)
        loc = vm.storage_location_uri
        with open(loc, mode="rb") as f:
            model = pickle.load(f)
            self.assertEqual(model.target_columns, ["IceCreamSales"])
            self.assertEqual(model.feature_columns, ["MeanDailyTemperature"])
            self.assertAlmostEqual(model.model.coef_[0], 255189.1, delta=0.1)
            self.assertAlmostEqual(model.model.intercept_, 79670.8, delta=0.1)
            # x1 = ice_cream_data["MeanDailyTemperature"].to_numpy().reshape(-1, 1)
            y1 = ice_cream_data["IceCreamSales"].to_numpy()
            y1_pred = model.predict(ice_cream_data)
            pred_np = y1_pred.to_numpy()
            score = r2_score(y1, pred_np)
            self.assertAlmostEqual(score, 0.7913, delta=0.0001)
            self.assertAlmostEqual(model.training_score, 0.7925, delta=0.0001)
            self.assertAlmostEqual(model.testing_score, 0.7865, delta=0.0001)
