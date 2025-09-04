import unittest
from unittest.mock import patch

from sklearn.linear_model import LinearRegression

from projects._03_independent_cqc._05a_model.model import Model, ModelType

PATCH_PATH = "projects._03_independent_cqc._05a_model.model"


class TestModel(unittest.TestCase):
    def test_model_linear_regression_instantiates(self):
        model = Model(
            model_type=ModelType.SIMPLE_LINEAR,
            model_identifier="test_linear_model",
            model_params={},
            version_parameter_location="/some/location",
            data_source_prefix="/some/prefix",
            target_column="target",
            feature_columns=["column1", "column2"],
        )
        self.assertEqual(model.model_type, ModelType.SIMPLE_LINEAR)
        self.assertEqual(model.model_identifier, "test_linear_model")
        self.assertEqual(model.model_params, {})
        self.assertEqual(model.version_parameter_location, "/some/location")
        self.assertEqual(model.data_source_prefix, "/some/prefix")
        self.assertEqual(model.target_column, "target")
        self.assertEqual(model.feature_columns, ["column1", "column2"])
        self.assertIsInstance(model.model, LinearRegression)
        self.assertEqual(model.training_score, None)
        self.assertEqual(model.testing_score, None)

    @patch(f"{PATCH_PATH}.ModelType", autospec=True)
    def test_model_linear_regression_raises_value_error_if_invalid_model_type(
        self, mock_model_type
    ):
        mock_model_type.SILLY_MODEL = "silly_model"
        with self.assertRaises(ValueError):
            model = Model(
                model_type=mock_model_type.SILLY_MODEL,
                model_identifier="test_linear_model",
                model_params={},
                version_parameter_location="/some/location",
                data_source_prefix="/some/prefix",
                target_column="target",
                feature_columns=["column1", "column2"],
            )
